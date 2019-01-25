import base64
import copy
import os
import tempfile
import warnings
from collections import namedtuple

import google.auth.transport.requests
import requests
import yaml

Config = namedtuple('Config', 'url token ca_cert client_cert version')
ClientCert = namedtuple('ClientCert', 'crt key')

FNAME_TOKEN = "/var/run/secrets/kubernetes.io/serviceaccount/token"
FNAME_CERT = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


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


def setup_requests(config: Config):
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


class DotDict(dict):
    """Dictionary that supports element access with '.'.

    Obviously, the usual Python rules apply. For instance, if

        dd = {'foo': 0, 'foo bar', 1': 1, 'items': 'xyz}`

    then this is valid:
    * `dd.foo`

    whereas these are not:
    * `dd.0`
    * `dd.foo bar`

    and
    * `dd.items`

    returns the `items` *method* of the underlying `dict`, not `dd["items"]`.

    """
    def __getattr__(self, key):
        return self[key]

    def __deepcopy__(self, *args, **kwargs):
        # To copy a `DotDict`, first convert it to a normal Python dict, then
        # let the `copy` module do its work and afterwards return a `DotDict`
        # version of that copy.
        return make_dotdict(copy.deepcopy(dict(self)))

    def __copy__(self, *args, **kwargs):
        return self.__deepcopy__(*args, **kwargs)


def make_dotdict(data):
    """Return `data` as a `DotDict`.

    This function will recursively replace all dictionary. It will also replace
    all tuples by lists.

    The intended input `data` of this function is any valid JSON structure.

    """
    if not isinstance(data, (list, tuple, dict)):
        return data

    # Recursively convert all elements in lists and dicts.
    if isinstance(data, (list, tuple)):
        return [make_dotdict(_) for _ in data]
    else:
        return DotDict({k: make_dotdict(v) for k, v in data.items()})


def undo_dotdict(data):
    """Remove all `DotDict` instances from `data`.

    This function will recursively replace all `DotDict` instances with their
    plain Python equivalent.

    """
    if not isinstance(data, (list, tuple, dict)):
        return data

    # Recursively convert all elements in lists and dicts.
    if isinstance(data, (list, tuple)):
        return [undo_dotdict(_) for _ in data]
    else:
        return dict({k: undo_dotdict(v) for k, v in data.items()})
