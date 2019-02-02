import json
import logging
import base64
import os
import tempfile
import warnings
from collections import namedtuple
from dtypes import RetVal

import google.auth.transport.requests
import requests
import yaml

Config = namedtuple('Config', 'url token ca_cert client_cert version')
ClientCert = namedtuple('ClientCert', 'crt key')

FNAME_TOKEN = "/var/run/secrets/kubernetes.io/serviceaccount/token"
FNAME_CERT = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


# Convenience: global logger instance to avoid repetitive code.
logit = logging.getLogger("square")


def load_incluster_config(fname_token=FNAME_TOKEN, fname_cert=FNAME_CERT):
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


def load_gke_config(kubeconfig, disable_warnings=False):
    """Return K8s access config for GKE cluster described in `kubeconfig`.

    Returns None if `kubeconfig` does not exist or could not be parsed.

    Inputs:
        kubconfig: str
            Name of kubeconfig file.
    Returns:
        Config

    """
    # Load `kubeconfig`. For this proof-of-concept we assume it contains
    # exactly one cluster and user.
    try:
        kubeconf = yaml.safe_load(open(kubeconfig))
    except FileNotFoundError:
        return None
    assert len(kubeconf['clusters']) == 1
    assert len(kubeconf['users']) == 1

    # Unpack the user and cluster info.
    cluster = kubeconf['clusters'][0]['cluster']
    user = kubeconf['users'][0]

    # Return immediately if this does not look like a config file for GKE.
    try:
        assert user['user']['auth-provider']['name'] == 'gcp'
    except (AssertionError, KeyError):
        return None

    # Unpack the self signed certificate (Google does not register the K8s API
    # server certificate with a public CA).
    ssl_ca_cert_data = base64.b64decode(cluster['certificate-authority-data'])

    # Save the certificate to a temporary file. This is only necessary because
    # the requests library needs a path to the CA file - unfortunately, we
    # cannot just pass it the content.
    _, ssl_ca_cert = tempfile.mkstemp(text=False)
    with open(ssl_ca_cert, 'wb') as fd:
        fd.write(ssl_ca_cert_data)

    # Authenticate with Compute Engine using the default project.
    with warnings.catch_warnings(record=disable_warnings):
        cred, project_id = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        cred.refresh(google.auth.transport.requests.Request())

    # Return the config data.
    return Config(
        url=cluster['server'],
        token=cred.token,
        ca_cert=ssl_ca_cert,
        client_cert=None,
        config=None,
    )


def load_minikube_config(kubeconfig):
    # Load `kubeconfig`. For this proof-of-concept we assume it contains
    # exactly one cluster and user.
    kubeconf = yaml.load(open(kubeconfig))
    assert len(kubeconf['clusters']) == 1
    assert len(kubeconf['users']) == 1

    # Unpack the user and cluster info.
    cluster = kubeconf['clusters'][0]
    user = kubeconf['users'][0]

    # Do not proceed if this does not look like a Minikube cluster.
    # Return immediately if this does not look like a config file for GKE.
    try:
        assert cluster['name'] == 'minikube'
    except (AssertionError, KeyError):
        return None

    # Minikube uses client certificates to authenticate. We need to pass those
    # to the HTTP client of our choice when we create the session.
    client_cert = ClientCert(
        crt=user['user']['client-certificate'],
        key=user['user']['client-key'],
    )

    # Return the config data.
    return Config(
        url=cluster['cluster']['server'],
        token=None,
        ca_cert=cluster['cluster']['certificate-authority'],
        client_cert=client_cert,
        version=None,
    )


def load_auto_config(kubeconfig: str, disable_warnings=False):
    """Automagically find and load the correct K8s configuration.

    This function will load several possible configuration options and returns
    the first one with a match. The order is as follows:

    1) `load_incluster_config`
    2) `load_gke_config`

    Inputs:
        kubconfig: str
            Name of kubeconfig file.
    Returns:
        Config

    """
    conf = load_incluster_config()
    if conf is not None:
        return conf

    conf = load_minikube_config(kubeconfig)
    if conf is not None:
        return conf

    conf = load_gke_config(kubeconfig, disable_warnings)
    if conf is not None:
        return conf

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


def request(client, method, path, payload, headers):
    """Return response of web request made with `client`.

    Inputs:
        client: `requests` session with correct K8s certificates.
        path: str
            Path to K8s resource (eg `/api/v1/namespaces`).
        payload: dict
            Anything that can be JSON encoded, usually a K8s manifest.
        headers: dict
            Request headers. These will *not* replace the existing request
            headers dictionary (eg the access tokens), but augment them.

    Returns:
        RetVal(dict, int): the JSON response and the HTTP status code.

    """
    try:
        ret = client.request(method, path, json=payload, headers=headers, timeout=30)
    except requests.exceptions.ConnectionError as err:
        method = err.request.method
        url = err.request.url
        logit.error(f"Connection error: {method} {url}")
        return RetVal(None, True)

    try:
        response = ret.json()
    except json.decoder.JSONDecodeError as err:
        msg = (
            f"JSON error: {err.msg} in line {err.lineno} column {err.colno}",
            "-" * 80 + "\n" + err.doc + "\n" + "-" * 80,
        )
        logit.error(str.join("\n", msg))
        return RetVal(None, True)

    return RetVal(response, ret.status_code)


def delete(client, path: str, payload: dict):
    """Make DELETE requests to K8s (see `k8s_request`)."""
    resp, code = request(client, 'DELETE', path, payload, headers=None)
    return RetVal(resp, code != 200)


def get(client, path: str):
    """Make GET requests to K8s (see `request`)."""
    resp, code = request(client, 'GET', path, payload=None, headers=None)
    return RetVal(resp, code != 200)


def patch(client, path: str, payload: dict):
    """Make PATCH requests to K8s (see `request`)."""
    headers = {'Content-Type': 'application/json-patch+json'}
    resp, code = request(client, 'PATCH', path, payload, headers)
    return RetVal(resp, code != 200)


def post(client, path: str, payload: dict):
    """Make POST requests to K8s (see `request`)."""
    resp, code = request(client, 'POST', path, payload, headers=None)
    return RetVal(resp, code != 201)


def version(config: Config, client):
    """Return new `config` with version number of K8s API.

    Contact the K8s API, query its version via `client` and return `config`
    with an updated `version` field. All other field in `config` will remain
    intact.

    Inputs:
        config: k8s.Config
        client: `requests` session with correct K8s certificates.

    Returns:
        k8s.Config

    """
    # Ask the K8s API for its version and check for errors.
    url = f"{config.url}/version"
    ret = get(client, url)
    if ret.err:
        return ret

    # Construct the version number of the K8s API.
    major, minor = ret.data['major'], ret.data['minor']
    version = f"{major}.{minor}"

    # Return an updated `Config` tuple.
    config = config._replace(version=version)
    return RetVal(config, None)
