import base64
import json
import logging
import os
import pathlib
import re
import subprocess
import tempfile
import warnings
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple, cast

import backoff
import google.auth
import google.auth.exceptions
import google.auth.transport.requests
import requests
import yaml

from square.dtypes import (
    Filepath, K8sClientCert, K8sConfig, K8sResource, MetaManifest,
)

FNAME_TOKEN = Filepath("/var/run/secrets/kubernetes.io/serviceaccount/token")
FNAME_CERT = Filepath("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")


# Convenience: global logger instance to avoid repetitive code.
logit = logging.getLogger("square")


def load_kubeconfig(fname: Filepath,
                    context: Optional[str]) -> Tuple[str, dict, dict, bool]:
    """Return user name and user- and cluster information.

    Return None on error.

    Inputs:
        fname: Filepath
            Path to kubeconfig file, eg "~/.kube/config.yaml"
        context: Optional[str]
            Kubeconf context. Use `None` to get the default context.

    Returns:
        name, user info, cluster info

    """
    # Load `kubeconfig`.
    try:
        kubeconf = yaml.safe_load(open(fname))
    except (IOError, PermissionError) as err:
        logit.error(f"{err}")
        return ("", {}, {}, True)

    # Find the user and cluster information based on the specified `context`.
    try:
        # Use default context unless specified.
        ctx = context if context else kubeconf["current-context"]
        del context

        try:
            # Find the correct context.
            ctx = [_ for _ in kubeconf["contexts"] if _["name"] == ctx]
            assert len(ctx) == 1
            ctx = ctx[0]["context"]

            # Unpack the cluster- and user name from the current context.
            clustername, username = ctx["cluster"], ctx["user"]

            # Find the information for the current cluster and user.
            user_info = [_ for _ in kubeconf["users"] if _["name"] == username]
            cluster_info = [_ for _ in kubeconf["clusters"] if _["name"] == clustername]
            assert len(user_info) == len(cluster_info) == 1
        except AssertionError:
            logit.error(f"Could not find information for context <{ctx}>")
            return ("", {}, {}, True)

        # Unpack the cluster- and user information.
        cluster_name = cluster_info[0]["name"]
        cluster_info_out = cluster_info[0]["cluster"]
        cluster_info_out["name"] = cluster_name
        user_info = user_info[0]["user"]
        del cluster_info
    except (KeyError, TypeError):
        logit.error(f"Kubeconfig YAML file <{fname}> is invalid")
        return ("", {}, {}, True)

    # Success. The explicit `dicts()` are to satisfy MyPy.
    logit.info(f"Loaded {ctx} from Kubeconfig file <{fname}>")
    return (username, dict(user_info), dict(cluster_info_out), False)


def load_incluster_config(
        fname_token: Filepath = FNAME_TOKEN,
        fname_cert: Filepath = FNAME_CERT) -> Tuple[K8sConfig, bool]:
    """Return K8s access config from Pod service account.

    Returns None if we are not running in a Pod.

    Inputs:
        kubconfig: str
            Name of kubeconfig file.
    Returns:
        Config

    """
    # Every K8s pod has this.
    server_ip = os.getenv('KUBERNETES_PORT_443_TCP_ADDR', None)

    fname_cert = pathlib.Path(fname_cert)
    fname_token = pathlib.Path(fname_token)

    # Sanity checks: URL and service account files either exist, or we are not
    # actually inside a Pod.
    try:
        assert server_ip is not None
        assert fname_cert.exists()
        assert fname_token.exists()
    except AssertionError:
        logit.debug("Could not find incluster (service account) credentials.")
        return K8sConfig(), True

    # Return the compiled K8s access configuration.
    logit.info("Use incluster (service account) credentials.")
    return K8sConfig(
        url=f'https://{server_ip}',
        token=fname_token.read_text(),
        ca_cert=fname_cert,
        client_cert=None,
        version="",
        name="",
    ), False


def load_gke_config(
        fname: Filepath,
        context: Optional[str],
        disable_warnings: bool = False) -> Tuple[K8sConfig, bool]:
    """Return K8s access config for GKE cluster described in `kubeconfig`.

    Returns None if `kubeconfig` does not exist or could not be parsed.

    Inputs:
        kubconfig: str
            Name of kubeconfig file.
        context: str
            Kubeconf context. Use `None` to use default context.
        disable_warnings: bool
            Whether or not do disable GCloud warnings.

    Returns:
        Config

    """
    # Parse the kubeconfig file.
    _, _, cluster, err = load_kubeconfig(fname, context)
    if err:
        return (K8sConfig(), True)

    # Unpack the self signed certificate (Google does not register the K8s API
    # server certificate with a public CA).
    try:
        ssl_ca_cert_data = base64.b64decode(cluster["certificate-authority-data"])
    except KeyError:
        logit.debug(f"Context {context} in <{fname}> is not a GKE config")
        return (K8sConfig(), True)

    # Save the certificate to a temporary file. This is only necessary because
    # the requests library will need a path to the CA file - unfortunately, we
    # cannot just pass it the content.
    _, tmp = tempfile.mkstemp(text=False)
    ssl_ca_cert = Filepath(tmp)
    ssl_ca_cert.write_bytes(ssl_ca_cert_data)

    with warnings.catch_warnings(record=disable_warnings):
        try:
            cred, _ = google.auth.default(
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
            cred.refresh(google.auth.transport.requests.Request())
            token = cred.token
        except google.auth.exceptions.DefaultCredentialsError as e:
            logit.error(str(e))
            return (K8sConfig(), True)

    # Return the config data.
    logit.info("Assuming GKE cluster.")
    return K8sConfig(
        url=cluster["server"],
        token=cast(str, token),
        ca_cert=ssl_ca_cert,
        client_cert=None,
        version="",
        name=cluster["name"],
    ), False


def load_eks_config(
        fname: Filepath,
        context: Optional[str],
        _: bool = False) -> Tuple[K8sConfig, bool]:
    """Return K8s access config for EKS cluster described in `kubeconfig`.

    Returns None if `kubeconfig` does not exist or could not be parsed.

    Inputs:
        fname: Filepath
            Kubeconfig file.
        context: str
            Kubeconf context. Use `None` to use default context.
        disable_warnings: bool (unused)
            Not used. It only exists for consistency with `load_gke_config` to
            make those functions generic.

    Returns:
        Config

    """
    # Parse the kubeconfig file.
    user, cluster, err = load_kubeconfig(fname, context)[1:]
    if err:
        return (K8sConfig(), True)

    # Get a copy of all env vars. We will pass that one along to the
    # sub-process, plus the env vars specified in the kubeconfig file.
    env = os.environ.copy()

    # Unpack the self signed certificate (AWS does not register the K8s API
    # server certificate with a public CA).
    try:
        ssl_ca_cert_data = base64.b64decode(cluster["certificate-authority-data"])
        cmd = user["exec"]["command"]
        args = user["exec"]["args"]
        env_kubeconf = user["exec"].get("env", [])
    except KeyError:
        logit.debug(f"Context {context} in <{fname}> is not an EKS config")
        return (K8sConfig(), True)

    # Convert a None value (valid value in YAML) to an empty list of env vars.
    env_kubeconf = env_kubeconf if env_kubeconf else []

    # Save the certificate to a temporary file. This is only necessary because
    # the Requests library will need a path to the CA file - unfortunately, we
    # cannot just pass it the content.
    tmp = tempfile.mkstemp(text=False)[1]
    ssl_ca_cert = Filepath(tmp)
    ssl_ca_cert.write_bytes(ssl_ca_cert_data)

    # Compile the name, arguments and env vars for the command specified in kubeconf.
    cmd_args = [cmd] + args
    env_kubeconf = {_["name"]: _["value"] for _ in env_kubeconf}
    env.update(env_kubeconf)
    logit.debug(f"Requesting EKS certificate: {cmd_args} with envs: {env_kubeconf}")

    # Pre-format the command for the log message.
    log_cmd = (
        f"kubeconf={fname} kubectx={context} "
        f"cmd={cmd_args}  env={env_kubeconf}"
    )

    # Run the specified command to produce the access token. That program must
    # produce a YAML document on stdout that specifies the bearer token.
    try:
        out = subprocess.run(cmd_args, stdout=subprocess.PIPE, env=env)
        token = yaml.safe_load(out.stdout.decode("utf8"))["status"]["token"]
    except FileNotFoundError:
        logit.error(f"Could not find {cmd} application to get token ({log_cmd})")
        return (K8sConfig(), True)
    except (KeyError, yaml.YAMLError):
        logit.error(f"Token manifest produce by {cmd_args} is corrupt ({log_cmd})")
        return (K8sConfig(), True)
    except TypeError:
        logit.error(f"The YAML token produced by {cmd_args} is corrupt ({log_cmd})")
        return (K8sConfig(), True)

    # Return the config data.
    logit.info("Assuming EKS cluster.")
    return K8sConfig(
        url=cluster["server"],
        token=token,
        ca_cert=ssl_ca_cert,
        client_cert=None,
        version="",
        name=cluster["name"],
    ), False


def load_minikube_config(fname: Filepath,
                         context: Optional[str]) -> Tuple[K8sConfig, bool]:
    """Load minikube configuration from `fname`.

    Return None on error.

    Inputs:
        fname: Filepath
            Path to kubeconfig file, eg "~/.kube/config.yaml"
        context: str
            Kubeconf context. Use `None` to select default context.

    Returns:
        Config

    """
    # Parse the kubeconfig file.
    _, user, cluster, err = load_kubeconfig(fname, context)
    if err:
        return (K8sConfig(), True)

    # Minikube uses client certificates to authenticate. We need to pass those
    # to the HTTP client of our choice when we create the session.
    try:
        client_cert = K8sClientCert(
            crt=Filepath(user["client-certificate"]),
            key=Filepath(user["client-key"]),
        )

        # Return the config data.
        logit.info("Assuming Minikube cluster.")
        return K8sConfig(
            url=cluster["server"],
            token="",
            ca_cert=Filepath(cluster["certificate-authority"]),
            client_cert=client_cert,
            version="",
            name=cluster["name"],
        ), False
    except KeyError:
        logit.debug(f"Context {context} in <{fname}> is not a Minikube config")
        return (K8sConfig(), True)


def load_kind_config(fname: Filepath, context: Optional[str]) -> Tuple[K8sConfig, bool]:
    """Load Kind configuration from `fname`.

    https://github.com/bsycorp/kind

    Kind is just another Minikube cluster. The only notable difference
    is that it does not store its credentials as files but directly in
    the Kubeconfig file. This function will copy those files into /tmp.

    Return None on error.

    Inputs:
        kubconfig: str
            Path to kubeconfig for Kind cluster.
        context: str
            Kubeconf context. Use `None` to use default context.

    Returns:
        Config

    """
    # Parse the kubeconfig file.
    _, user, cluster, err = load_kubeconfig(fname, context)
    if err:
        return (K8sConfig(), True)

    # Kind and Minikube use client certificates to authenticate. We need to
    # pass those to the HTTP client of our choice when we create the session.
    try:
        client_crt = base64.b64decode(user["client-certificate-data"]).decode()
        client_key = base64.b64decode(user["client-key-data"]).decode()
        client_ca = base64.b64decode(cluster["certificate-authority-data"]).decode()
        path = Filepath(tempfile.mkdtemp())
        p_client_crt = path / "kind-client.crt"
        p_client_key = path / "kind-client.key"
        p_ca = path / "kind.ca"
        p_client_crt.write_text(client_crt)
        p_client_key.write_text(client_key)
        p_ca.write_text(client_ca)
        client_cert = K8sClientCert(crt=p_client_crt, key=p_client_key)

        # Return the config data.
        logit.debug("Assuming Minikube/Kind cluster.")
        return K8sConfig(
            url=cluster["server"],
            token="",
            ca_cert=p_ca,
            client_cert=client_cert,
            version="",
            name=cluster["name"],
        ), False
    except KeyError:
        logit.debug(f"Context {context} in <{fname}> is not a Minikube config")
        return (K8sConfig(), True)


def load_auto_config(
        fname: Filepath,
        context: Optional[str],
        disable_warnings: bool = False) -> Tuple[K8sConfig, bool]:
    """Automagically find and load the correct K8s configuration.

    This function will load several possible configuration options and returns
    the first one with a match. The order is as follows:

    1) `load_incluster_config`
    2) `load_gke_config`

    Inputs:
        fname: str
            Path to kubeconfig file, eg "~/.kube/config.yaml"
            Use `None` to find out automatically or for incluster credentials.

        context: str
            Kubeconf context. Use `None` to use default context.

    Returns:
        Config

    """
    conf, err = load_incluster_config()
    if not err:
        return conf, False
    logit.debug("Incluster config failed")

    conf, err = load_minikube_config(fname, context)
    if not err:
        return conf, False
    logit.debug("Minikube config failed")

    conf, err = load_kind_config(fname, context)
    if not err:
        return conf, False
    logit.debug("KIND config failed")

    conf, err = load_eks_config(fname, context, disable_warnings)
    if not err:
        return conf, False
    logit.debug("EKS config failed")

    conf, err = load_gke_config(fname, context, disable_warnings)
    if not err:
        return conf, False
    logit.debug("GKE config failed")

    logit.error(f"Could not find a valid configuration in <{fname}>")
    return (K8sConfig(), True)


def session(k8sconfig: K8sConfig):
    """Return configured `requests` session."""
    # Plain session.
    sess = requests.Session()

    # Load the CA file (necessary for self signed certs to avoid https warning).
    sess.verify = str(k8sconfig.ca_cert)

    # Add the client certificate, if the cluster uses those to authenticate users.
    if k8sconfig.client_cert is not None:
        sess.cert = (str(k8sconfig.client_cert.crt), str(k8sconfig.client_cert.key))

    # Add the bearer token if this cluster uses them to authenticate users.
    if k8sconfig.token is not None:
        sess.headers.update({'authorization': f'Bearer {k8sconfig.token}'})

    # Return the configured session object.
    return sess


def resource(k8sconfig: K8sConfig, meta: MetaManifest) -> Tuple[K8sResource, bool]:
    """Return `K8sResource` object.

    That object will contain the full path to a resource, eg.
    https://1.2.3.4/api/v1/namespace/foo/services.

    Inputs:
        k8sconfig: K8sConfig
        meta: MetaManifest

    Returns:
        K8sResource

    """
    err_resp = (K8sResource("", "", "", False, ""), True)

    # Compile the lookup key for the resource, eg `("Service", "v1")`.
    if not meta.apiVersion:
        # Use the most recent version of the API if None was specified.
        candidates = [(kind, ver) for kind, ver in k8sconfig.apis if kind == meta.kind]
        if len(candidates) == 0:
            logit.warning(f"Cannot determine API version for <{meta.kind}>")
            return err_resp
        candidates.sort()
        key = candidates.pop(0)
    else:
        key = (meta.kind, meta.apiVersion)

    # Retrieve the resource.
    try:
        resource = k8sconfig.apis[key]
    except KeyError:
        logit.error(f"Unsupported resource <{meta.kind}> {key}.")
        return err_resp

    # Void the "namespace" key for non-namespaced resources.
    if not resource.namespaced:
        meta = meta._replace(namespace=None)

    # Namespaces are special because they lack the `namespaces/` path prefix.
    if meta.kind == "Namespace":
        # Return the correct URL, depending on whether we want all namespaces
        # or a particular one.
        url = f"{resource.url}/namespaces"
        if meta.name:
            url += f"/{meta.name}"
        return resource._replace(url=url), False

    # Determine the prefix for namespaced resources.
    if meta.namespace is None:
        namespace = ""
    else:
        # Namespace name must conform to K8s standards.
        match = re.match(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", meta.namespace)
        if match is None or match.group() != meta.namespace:
            logit.error(f"Invalid namespace name <{meta.namespace}>.")
            return err_resp
        namespace = f"namespaces/{meta.namespace}"

    # Sanity check: we cannot search for a namespaced resource by name in all
    # namespaces. Example: we cannot search for a Service `foo` in all
    # namespaces. We could only search for Service `foo` in namespace `bar`, or
    # all services in all namespaces.
    if resource.namespaced and meta.name and not meta.namespace:
        logit.error(f"Cannot search for {meta.kind} {meta.name} in {meta.namespace}")
        return err_resp

    # Create the full path to the resource depending on whether we have a
    # namespace and resource name. Here are all three possibilities:
    #  - /api/v1/namespaces/services
    #  - /api/v1/namespaces/my-namespace/services
    #  - /api/v1/namespaces/my-namespace/services/my-service
    path = f"{namespace}/{resource.name}" if namespace else resource.name
    path = f"{path}/{meta.name}" if meta.name else path

    # The concatenation above may have introduced `//`. Here we remove them.
    path = path.replace("//", "/")

    # Return the K8sResource with the correct URL.
    resource = resource._replace(url=f"{resource.url}/{path}")
    return resource, False


def request(
        client,
        method: str,
        url: str,
        payload: Optional[dict | list],
        headers: Optional[dict]) -> Tuple[dict, bool]:
    """Return response of web request made with `client`.

    Inputs:
        client: `requests` session with correct K8s certificates.
        url: str
            Eg `https://1.2.3.4/api/v1/namespaces`)
        payload: dict
            Anything that can be JSON encoded, usually a K8s manifest.
        headers: dict
            Request headers. These will *not* replace the existing request
            headers dictionary (eg the access tokens), but augment them.

    Returns:
        (dict, int): the JSON response and the HTTP status code.

    """
    # Define the maximum number of tries and exceptions we want to retry on.
    max_tries = 21
    web_exceptions = (requests.exceptions.RequestException, )

    def on_backoff(details):
        """Log a warning whenever we retry."""
        tries = details["tries"]
        logit.warning(f"Backing off on {url} (Attempt {tries}/{max_tries-1})")

    """
    Use linear backoff. The backoff is not exponential because the most
    prevalent use case for this backoff we have seen so far is to wait for
    new resource endpoints to become available. These may take a few seconds,
    or tens of seconds to do so. If we used an exponential strategy we may
    end up waiting for a very long time for no good reason. The time between
    backoffs is fairly large to avoid hammering the API. Jitter is disabled
    because the irregular intervals are irritating in an interactive tool.
    """
    @backoff.on_exception(backoff.constant, web_exceptions,
                          max_tries=max_tries,
                          interval=3,
                          max_time=20,
                          on_backoff=on_backoff,
                          logger=None,  # type: ignore
                          jitter=None,  # type: ignore
                          )
    def _call(*args, **kwargs):
        return client.request(method, url, json=payload, headers=headers, timeout=30)

    # Make the web request via our backoff/retry handler.
    try:
        ret = _call()
    except web_exceptions as err:
        logit.error(f"{err} ({method} {url})")
        return ({}, True)

    try:
        response = json.loads(ret.text)
    except json.decoder.JSONDecodeError as err:
        msg = (
            f"JSON error: {err.msg} in line {err.lineno} column {err.colno}",
            "-" * 80 + "\n" + err.doc + "\n" + "-" * 80,
        )
        logit.error(str.join("\n", msg))
        return ({}, True)

    # Log the entire request in debug mode.
    logit.debug(
        f"{method} {ret.status_code} {ret.url}\n"
        f"Headers: {headers}\n"
        f"Payload: {payload}\n"
        f"Response: {response}\n"
    )
    return (response, ret.status_code)


def delete(client, url: str, payload: dict) -> Tuple[dict, bool]:
    """Make DELETE requests to K8s (see `k8s_request`)."""
    resp, code = request(client, 'DELETE', url, payload, headers=None)
    err = (code not in (200, 202))
    if err:
        logit.error(f"{code} - DELETE - {url} - {resp}")
    return (resp, err)


def get(client, url: str) -> Tuple[dict, bool]:
    """Make GET requests to K8s (see `request`)."""
    resp, code = request(client, 'GET', url, payload=None, headers=None)
    err = (code != 200)
    if err:
        logit.error(f"{code} - GET - {url} - {resp}")
    return (resp, err)


def patch(client, url: str, payload: List[Dict[str, str]]) -> Tuple[dict, bool]:
    """Make PATCH requests to K8s (see `request`)."""
    headers = {'Content-Type': 'application/json-patch+json'}
    resp, code = request(client, 'PATCH', url, payload, headers)
    err = (code != 200)
    if err:
        logit.error(f"{code} - PATCH - {url} - {resp}")
    return (resp, err)


def post(client, url: str, payload: dict) -> Tuple[dict, bool]:
    """Make POST requests to K8s (see `request`)."""
    resp, code = request(client, 'POST', url, payload, headers=None)
    err = (code != 201)
    if err:
        logit.error(f"{code} - POST - {url} - {resp}")
    return (resp, err)


def version(k8sconfig: K8sConfig) -> Tuple[K8sConfig, bool]:
    """Return new `k8sconfig` with version number of K8s API.

    Contact the K8s API, query its version via `client` and return `k8sconfig`
    with an updated `version` field. All other field in `k8sconfig` will remain
    intact.

    Inputs:
        k8sconfig: K8sConfig

    Returns:
        K8sConfig

    """
    # Ask the K8s API for its version and check for errors.
    url = f"{k8sconfig.url}/version"
    resp, err = get(k8sconfig.client, url)
    if err or resp is None:
        logit.error(f"Could not interrogate {k8sconfig.name} ({url})")
        return (K8sConfig(), True)

    # Construct the version number of the K8s API.
    major, minor = resp['major'], resp['minor']
    version = f"{major}.{minor}"

    # If we are talking to GKE, the version string may now be "1.10+". It
    # simply indicates that GKE is running version 1.10.x. We need to remove
    # the "+" because the version string is important in `square`, for instance
    # to determines which URLs to contact, which fields are valid.
    version = version.replace("+", "")

    # Return an updated `K8sconfig` tuple.
    k8sconfig = k8sconfig._replace(version=version)
    return (k8sconfig, False)


def cluster_config(
        kubeconfig: Filepath,
        context: Optional[str]) -> Tuple[K8sConfig, bool]:
    """Return web session to K8s API.

    This will read the Kubernetes credentials, contact Kubernetes to
    interrogate its version and then return the configuration and web-session.

    Inputs:
        kubeconfig: str
            Path to kubeconfig file.
        context: str
            Kubernetes context to use (can be `None` to use default).

    Returns:
        K8sConfig

    """
    # Read Kubeconfig file and use it to create a `requests` client session.
    # That session will have the proper security certificates and headers so
    # that subsequent calls to K8s need not deal with it anymore.
    kubeconfig = kubeconfig.expanduser()
    try:
        # Parse Kubeconfig file.
        k8sconfig, err = load_auto_config(kubeconfig, context, disable_warnings=True)
        assert not err

        # Configure web session.
        k8sconfig = k8sconfig._replace(client=session(k8sconfig))
        assert k8sconfig.client

        # Contact the K8s API to update version field in `k8sconfig`.
        k8sconfig, err = version(k8sconfig)
        assert not err and k8sconfig

        # Populate the `k8sconfig.apis` field.
        err = compile_api_endpoints(k8sconfig)
        assert not err
    except AssertionError:
        return (K8sConfig(), True)

    # Log the K8s API address and version.
    logit.info(f"Kubernetes server at {k8sconfig.url}")
    logit.info(f"Kubernetes version is {k8sconfig.version}")
    return (k8sconfig, False)


def parse_api_group(api_version, url, resp) -> Tuple[List[K8sResource], Dict[str, str]]:
    """Compile the K8s API `resp` into a `K8sResource` tuples.

    The `resp` is the verbatim response from the K8s API group regarding the
    resources it provides. Here we compile those into `K8sResource` tuples iff
    they meet the criteria to be manageable by Square. These criteria are, most
    notably, the ability to create, get, patch and delete the resource.

    Also return a LUT to convert short names like "svc" into the proper resource
    kind "Service".

    """
    resources = resp["resources"]

    def valid(_res):
        """Return `True` if `res` describes a Square compatible resource."""
        # Convenience.
        name = _res["name"]
        verbs = list(sorted(_res["verbs"]))

        # Ignore resource like "services/status". We only care for "services".
        if "/" in name:
            logit.debug(f"Ignore resource <{name}>: has a slash ('/') in its name")
            return False

        # Square can only man age the resource if it can be read, modified and
        # deleted. Here we check if `res` has the respective verbs.
        minimal_verbs = {"create", "delete", "get", "list", "patch", "update"}
        if not minimal_verbs.issubset(set(verbs)):
            logit.debug(f"Ignore resource <{name}>: insufficient verbs: {verbs}")
            return False

        return True

    # Compile the K8s resource definition into a `K8sResource` structure if it
    # is compatible with Square (see `valid` helper above).
    group_urls: List[K8sResource]
    group_urls = [
        K8sResource(api_version, _["kind"], _["name"], _["namespaced"], url)
        for _ in resources if valid(_)
    ]

    # Compile LUT to translate short names into their proper resource
    # kind: Example short2kind = {"service":, "Service", "svc": "Service"}
    short2kind: Dict[str, str] = {}
    for res in resources:
        kind = res["kind"]

        short2kind[kind.lower()] = kind
        short2kind[res["name"]] = kind
        for short_name in res.get("shortNames", []):
            short2kind[short_name] = kind

    return (group_urls, short2kind)


def compile_api_endpoints(k8sconfig: K8sConfig) -> bool:
    """Populate `k8sconfig.apis` with all the K8s endpoints`.

    NOTE: This will purge the existing content in `k8sconfig.apis`.

    Returns a dictionary like the following:
    {
      ('ConfigMap', 'v1'): K8sResource(
        apiVersion=v1, kind='ConfigMap', name='configmaps', namespaced=True,
        url='https://localhost:8443/api/v1/configmaps'),
      ('CronJob', 'batch/v1beta1): K8sResource(
        apiVersion='batch/v1beta1', kind='CronJob', name='cronjobs', namespaced=True,
        url='https://localhost:8443/apis/batch/v1beta1/cronjobs'),
      ('DaemonSet', 'apps/v1'): K8sResource(
        apiVersion='apps/v1', kind='DaemonSet', name='daemonsets', namespaced=True,
        url='https://localhost:8443/apis/apps/v1/daemonsets',
      ('DaemonSet', apps/v1beta1): K8sResource(
        apiVersion='apps/v1beta1', kind='DaemonSet', name='daemonsets', namespaced=True,
        url='https://localhost:8443/apis/extensions/v1beta1/daemonsets'),
    }

    Inputs:
        k8sconfig: K8sConfig

    """
    # Compile the list of all K8s API groups that this K8s instance knows about.
    resp, err = get(k8sconfig.client, f"{k8sconfig.url}/apis")
    if err:
        logit.error(f"Could not interrogate {k8sconfig.name} ({k8sconfig.url}/apis)")
        return True

    # Compile the list of all API groups and their endpoints. Example
    # apigroups = {
    #     'extensions': {('extensions/v1beta1', 'apis/extensions/v1beta1')},
    #     'apps': {('apps/v1', 'apis/apps/v1'),
    #              ('apps/v1beta1', 'apis/apps/v1beta1'),
    #              ('apps/v1beta2', 'apis/apps/v1beta2')},
    #     'batch': {('batch/v1', 'apis/batch/v1'),
    #               ('batch/v1beta1', 'apis/batch/v1beta1')},
    #     ...
    # }
    apigroups: Dict[str, Set[Tuple[str, str]]] = {}
    preferred_group: Dict[str, str] = {}
    for group in resp["groups"]:
        name = group["name"]

        # Store the preferred version, eg ("", "apis/v1").
        apigroups[name] = set()

        # Compile all alternative versions into the same set.
        for version in group["versions"]:
            ver = version["groupVersion"]
            apigroups[name].add((ver, f"apis/{ver}"))
            preferred_group[ver] = group["preferredVersion"]["groupVersion"]
        del group

    # The "v1" group comprises the traditional core components like Service and
    # Pod. This group is a special case and exposed under "api/v1" instead
    # of the usual `apis/...` path.
    apigroups["v1"] = {("v1", "api/v1")}
    preferred_group["v1"] = "v1"

    # Contact K8s to find out which resources each API group offers.
    # This will produce the following group_urls below (K = `K8sResource`): {
    #  ('apps', 'apps/v1', 'apis/apps/v1'): [
    #   K(*, kind='DaemonSet', name='daemonsets', namespaced=True, url='apis/apps/v1'),
    #   K(*, kind='Deployment', name='deployments', namespaced=True, url='apis/apps/v1'),
    #   K(*, kind='ReplicaSet', name='replicasets', namespaced=True, url='apis/apps/v1'),
    #   K(*, kind='StatefulSet', name='statefulsets', namespaced=True, url='apis/apps/v1')
    #  ],
    #  ('apps', 'apps/v1beta1', 'apis/apps/v1beta1')': [
    #   K(..., kind='Deployment', name='deployments', namespaced=True, url=...),
    #   K(..., kind='StatefulSet', name='statefulsets', namespaced=True, url=...)
    #  ],
    # }
    group_urls: Dict[Tuple[str, str, str], List[K8sResource]] = {}
    for group_name, ver_url in apigroups.items():
        for api_version, url in ver_url:
            resp, err = get(k8sconfig.client, f"{k8sconfig.url}/{url}")
            if err:
                msg = f"Could not interrogate {k8sconfig.name} ({k8sconfig.url}/{url})"
                logit.error(msg)
                return True

            data, short2kind = parse_api_group(api_version, url, resp)
            group_urls[(group_name, api_version, url)] = data
            k8sconfig.short2kind.update(short2kind)

    # Produce the entries for `K8sConfig.apis` as described in the doc string.
    k8sconfig.apis.clear()
    default: Dict[str, Set[K8sResource]] = defaultdict(set)
    for (group_name, api_version, url), resources in group_urls.items():
        for res in resources:
            key = (res.kind, res.apiVersion)  # fixme: define namedtuple
            k8sconfig.apis[key] = res._replace(url=f"{k8sconfig.url}/{res.url}")

            if preferred_group[api_version] == api_version:
                default[res.kind].add(k8sconfig.apis[key])
                k8sconfig.apis[(res.kind, "")] = k8sconfig.apis[key]

    # Determine the default API endpoint Square should query for each resource.
    for kind, resources in default.items():
        # Happy case: the resource is only available from a single API group.
        if len(resources) == 1:
            k8sconfig.apis[(kind, "")] = resources.pop()
            continue

        # If we get here then it means a resource is available from different
        # API groups. Here we use heuristics to pick one. The heuristic is
        # simply to look for one that is neither alpha nor beta. In Kubernetes
        # v1.15 this resolves almost all disputes.
        all_apis = list(sorted([_.apiVersion for _ in resources]))

        # Remove all alpha/beta resources.
        prod_apis = [_ for _ in all_apis if not ("alpha" in _ or "beta" in _)]

        # Re-add the alpha/beta resources to the candidate set if we have no
        # production ones to choose from.
        apis = prod_apis if len(prod_apis) > 0 else list(all_apis)

        # Pick the one with probably the highest version number.
        apis.sort()
        version = apis.pop()
        k8sconfig.apis[(kind, "")] = [_ for _ in resources if _.apiVersion == version][0]

        # Log the available options. Mark the one Square chose with a "*".
        tmp = [_ if _ != version else f"*{_}*" for _ in all_apis]
        logit.info(f"Ambiguous {kind.upper()} endpoints: {tmp}")

    # Compile the set of all resource kinds that this Kubernetes cluster supports.
    for kind, _ in k8sconfig.apis:
        k8sconfig.kinds.add(kind)
    return False
