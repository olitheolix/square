import base64
import json
import logging
import os
import re
import tempfile
import warnings
from collections import namedtuple
from typing import Optional, Tuple

import google.auth
import google.auth.transport.requests
import requests
import yaml
from dtypes import SUPPORTED_KINDS, SUPPORTED_VERSIONS, Config, Filepath

ClientCert = namedtuple('ClientCert', 'crt key')

FNAME_TOKEN = "/var/run/secrets/kubernetes.io/serviceaccount/token"
FNAME_CERT = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


# Convenience: global logger instance to avoid repetitive code.
logit = logging.getLogger("square")


def load_kubeconfig(
        fname: Filepath,
        context: Optional[str]
) -> Tuple[Optional[str], Optional[dict], Optional[dict]]:
    """Return user name and user- and cluster information.

    Return None on error.

    Inputs:
        fname: str
            Path to kubeconfig file, eg "~/.kube/config.yaml"
        context: str
            Kubeconf context. Use `None` to use default context.

    Returns:
        name, user info, cluster info

    """
    # Load `kubeconfig`.
    try:
        kubeconf = yaml.load(open(fname))
    except (IOError, PermissionError) as err:
        logit.error(f"{err}")
        return (None, None, None)

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
            return (None, None, None)

        # Unpack the cluster and user information.
        cluster_info = cluster_info[0]["cluster"]
        user_info = user_info[0]["user"]
    except KeyError:
        logit.error(f"Kubeconfig YAML file <{fname}> is invalid")
        return (None, None, None)

    # Success. The explicit `dicts()` are to satisfy MyPy.
    return (username, dict(user_info), dict(cluster_info))


def load_incluster_config(
        fname_token: str = FNAME_TOKEN,
        fname_cert: str = FNAME_CERT) -> Optional[Config]:
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

    # Sanity checks: URL and service account files either exist, or we are not
    # actually inside a Pod.
    try:
        assert server_ip is not None
        assert os.path.exists(fname_cert)
        assert os.path.exists(fname_token)
    except AssertionError:
        return None

    # Return the compiled K8s access configuration.
    try:
        conf = Config(
            url=f'https://{server_ip}',
            token=open(fname_token, 'r').read(),
            ca_cert=fname_cert,
            client_cert=None,
            version=None,
        )
        return conf
    except FileNotFoundError:
        return None


def load_gke_config(
        fname: Filepath,
        context: Optional[str],
        disable_warnings: bool = False) -> Optional[Config]:
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
    name, user, cluster = load_kubeconfig(fname, context)
    if name is None or user is None or cluster is None:
        return None

    # Unpack the self signed certificate (Google does not register the K8s API
    # server certificate with a public CA).
    try:
        ssl_ca_cert_data = base64.b64decode(
            cluster["certificate-authority-data"]
        )
    except KeyError:
        logit.debug(f"Context {context} in <{fname}> is not a GKE config")
        return None

    # Save the certificate to a temporary file. This is only necessary because
    # the requests library needs a path to the CA file - unfortunately, we
    # cannot just pass it the content.
    _, ssl_ca_cert = tempfile.mkstemp(text=False)
    with open(ssl_ca_cert, "wb") as fd:
        fd.write(ssl_ca_cert_data)

    # Get the access token from Compute Engine (uses the default project).
    with warnings.catch_warnings(record=disable_warnings):
        cred, project_id = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        cred.refresh(google.auth.transport.requests.Request())

    # Return the config data.
    return Config(
        url=cluster["server"],
        token=cred.token,
        ca_cert=ssl_ca_cert,
        client_cert=None,
        version=None,
    )


def load_minikube_config(
        fname: Filepath,
        context: Optional[str],
) -> Optional[Config]:
    """Load minikube configuration from `fname`.

    Return None on error.

    Inputs:
        kubconfig: str
            Path to kubeconfig file, eg "~/.kube/config.yaml"
        context: str
            Kubeconf context. Use `None` to use default context.

    Returns:
        Config

    """
    # Parse the kubeconfig file.
    name, user, cluster = load_kubeconfig(fname, context)
    if name is None or user is None or cluster is None:
        return None

    # Minikube uses client certificates to authenticate. We need to pass those
    # to the HTTP client of our choice when we create the session.
    try:
        client_cert = ClientCert(
            crt=user["client-certificate"],
            key=user["client-key"],
        )

        # Return the config data.
        return Config(
            url=cluster["server"],
            token=None,
            ca_cert=cluster["certificate-authority"],
            client_cert=client_cert,
            version=None,
        )
    except KeyError:
        logit.debug(f"Context {context} in <{fname}> is not a Minikube config")
        return None


def load_auto_config(
        fname: Filepath,
        context: Optional[str],
        disable_warnings: bool = False) -> Optional[Config]:
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
    conf = load_incluster_config()
    if conf is not None:
        return conf
    logit.debug("Incluster config failed")

    conf = load_minikube_config(fname, context)
    if conf is not None:
        return conf
    logit.debug("Minikube config failed")

    conf = load_gke_config(fname, context, disable_warnings)
    if conf is not None:
        return conf
    logit.debug("GKE config failed")

    logit.error(f"Could not find a valid configuration in <{fname}>")
    return None


def session(config: Config):
    """Return configured `requests` session."""
    # Plain session.
    sess = requests.Session()

    # Load the CA file (necessary for self signed certs to avoid https warning).
    sess.verify = config.ca_cert

    # Add the client certificate, if the cluster uses those to authenticate users.
    if config.client_cert is not None:
        sess.cert = (config.client_cert.crt, config.client_cert.key)

    # Add the bearer token if this cluster uses them to authenticate users.
    if config.token is not None:
        sess.headers = {'authorization': f'Bearer {config.token}'}

    # Return the configured session object.
    return sess


def urlpath(
        config: Config,
        kind: str,
        namespace: Optional[str]) -> Tuple[Optional[str], bool]:
    """Return complete URL to K8s resource.

    Inputs:
        config: k8s.Config
        kind: str
            Eg "Deployment", "Namespace", ... (case sensitive).
        namespace: str
            Must be None for "Namespace" resources.

    Returns:
        str: Full path K8s resource, eg https://1.2.3.4/api/v1/namespace/foo/services.

    """
    # Namespaces are special because they lack the `namespaces/` path prefix.
    if kind == "Namespace" or namespace is None:
        namespace = ""
    else:
        # Namespace name must conform to K8s standards.
        match = re.match(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", namespace)
        if match is None or match.group() != namespace:
            logit.error(f"Invalid namespace name <{namespace}>.")
            return (None, True)

        namespace = f"namespaces/{namespace}/"

    # We must support the specified resource kind.
    if kind not in SUPPORTED_KINDS:
        logit.error(f"Unsupported resource <{kind}>.")
        return (None, True)

    # We must support the specified K8s version.
    if config.version not in SUPPORTED_VERSIONS:
        logit.error(f"Unsupported K8s version <{config.version}>.")
        return (None, True)

    # The HTTP request path names by K8s version and resource kind.
    # The keys in this dict must cover all those specified in
    # `SUPPORTED_VERSIONS` and `SUPPORTED_KINDS`.
    resources = {
        "1.9": {
            "ConfigMap": f"api/v1/{namespace}/configmaps",
            "Secret": f"api/v1/{namespace}/secrets",
            "Deployment": f"apis/extensions/v1beta1/{namespace}/deployments",
            "Ingress": f"apis/extensions/v1beta1/{namespace}/ingresses",
            "Namespace": f"api/v1/namespaces",
            "Service": f"api/v1/{namespace}/services",
        },
        "1.10": {
            "ConfigMap": f"api/v1/{namespace}/configmaps",
            "Secret": f"api/v1/{namespace}/secrets",
            "Deployment": f"apis/extensions/v1beta1/{namespace}/deployments",
            "Ingress": f"apis/extensions/v1beta1/{namespace}/ingresses",
            "Namespace": f"api/v1/namespaces",
            "Service": f"api/v1/{namespace}/services",
        },
        "1.11": {
            "ConfigMap": f"api/v1/{namespace}/configmaps",
            "Secret": f"api/v1/{namespace}/secrets",
            "Deployment": f"apis/apps/v1/{namespace}/deployments",
            "Ingress": f"apis/extensions/v1beta1/{namespace}/ingresses",
            "Namespace": f"api/v1/namespaces",
            "Service": f"api/v1/{namespace}/services",
        },
    }

    # Look up the resource path and remove duplicate "/" characters (may have
    # slipped in when no namespace was supplied, eg "/api/v1//configmaps").
    path = resources[config.version][kind]
    path = path.replace("//", "/")
    assert not path.startswith("/")

    # Return the complete resource URL.
    return (f"{config.url}/{path}", False)


def request(
        client,
        method: str,
        url: str,
        payload: Optional[dict],
        headers: Optional[dict]) -> Tuple[Optional[dict], bool]:
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
    try:
        ret = client.request(method, url, json=payload, headers=headers, timeout=30)
    except requests.exceptions.ConnectionError as err:
        method = err.request.method
        url = err.request.url
        logit.error(f"Connection error: {method} {url}")
        return (None, True)

    try:
        response = ret.json()
    except json.decoder.JSONDecodeError as err:
        msg = (
            f"JSON error: {err.msg} in line {err.lineno} column {err.colno}",
            "-" * 80 + "\n" + err.doc + "\n" + "-" * 80,
        )
        logit.error(str.join("\n", msg))
        return (None, True)

    # Log the entire request in debug mode.
    logit.debug(
        f"{method} {ret.status_code} {ret.url}\n"
        f"Headers: {headers}\n"
        f"Payload: {payload}\n"
        f"Response: {response}\n"
    )
    return (response, ret.status_code)


def delete(client, url: str, payload: dict) -> Tuple[Optional[dict], bool]:
    """Make DELETE requests to K8s (see `k8s_request`)."""
    resp, code = request(client, 'DELETE', url, payload, headers=None)
    err = (code not in (200, 202))
    if err:
        logit.error(f"{code} - DELETE - {url}")
    return (resp, err)


def get(client, url: str) -> Tuple[Optional[dict], bool]:
    """Make GET requests to K8s (see `request`)."""
    resp, code = request(client, 'GET', url, payload=None, headers=None)
    err = (code != 200)
    if err:
        logit.error(f"{code} - GET - {url}")
    return (resp, err)


def patch(client, url: str, payload: dict) -> Tuple[Optional[dict], bool]:
    """Make PATCH requests to K8s (see `request`)."""
    headers = {'Content-Type': 'application/json-patch+json'}
    resp, code = request(client, 'PATCH', url, payload, headers)
    err = (code != 200)
    if err:
        logit.error(f"{code} - PATCH - {url}")
    return (resp, err)


def post(client, url: str, payload: dict) -> Tuple[Optional[dict], bool]:
    """Make POST requests to K8s (see `request`)."""
    resp, code = request(client, 'POST', url, payload, headers=None)
    err = (code != 201)
    if err:
        logit.error(f"{code} - POST - {url}")
    return (resp, err)


def version(config: Config, client) -> Tuple[Optional[Config], bool]:
    """Return new `config` with version number of K8s API.

    Contact the K8s API, query its version via `client` and return `config`
    with an updated `version` field. All other field in `config` will remain
    intact.

    Inputs:
        config: Config
        client: `requests` session with correct K8s certificates.

    Returns:
        Config

    """
    # Ask the K8s API for its version and check for errors.
    url = f"{config.url}/version"
    resp, err = get(client, url)
    if err or resp is None:
        return (None, True)

    # Construct the version number of the K8s API.
    major, minor = resp['major'], resp['minor']
    version = f"{major}.{minor}"

    # If we are talking to GKE, the version string may now be "1.10+". It
    # simply indicates that GKE is running version 1.10.x. We need to remove
    # the "+" because the version string is important in `square`, for instance
    # to determines which URLs to contact, which fields are valid.
    version = version.replace("+", "")

    # Return an updated `Config` tuple.
    config = config._replace(version=version)
    return (config, False)
