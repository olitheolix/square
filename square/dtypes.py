import pathlib
from typing import (
    Any, Collection, Dict, Iterable, NamedTuple, Optional, Set, Tuple,
)

# We support these resource types. The order matters because it determines the
# order in which the manifests will be grouped in the output files.
SUPPORTED_KINDS = (
    # Namespaces must come first to ensure the other resources can be created
    # within them.
    "Namespace",

    # Configuration and PVC before Deployments & friends use them.
    "ConfigMap", "Secret", "PersistentVolumeClaim",

    # RBAC.
    "ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding", "ServiceAccount",

    # Define Services before creating Deployments & friends.
    "Service",

    # Everything that will spawn pods.
    "CronJob", "Deployment", "DaemonSet", "StatefulSet", "HorizontalPodAutoscaler",

    # Ingresses should be after Deployments & friends.
    "Ingress",
)


# Declare aliases for each resource type. Will be used in command line parsing
# to save the user some typing and match what `kubectl` would accept. We do not
# need to worry about capitalisation because `square.parse_commandline_args`
# will always convert everything to lower case letters first.
RESOURCE_ALIASES = {
    "ClusterRole": {"clusterrole", "clusterroles"},
    "ClusterRoleBinding": {"clusterrolebinding", "clusterrolebindings"},
    "ConfigMap": {"configmap", "cm"},
    "CronJob": {"cronjob", "cj"},
    "DaemonSet": {"daemonset", "daemonsets", "ds"},
    "Deployment": {"deployment", "deployments", "deploy"},
    "HorizontalPodAutoscaler": {"hpa"},
    "Ingress": {"ingress", "ingresses", "ing"},
    "Namespace": {"namespace", "namespaces", "ns"},
    "PersistentVolumeClaim": {"persistentVolumeClaim", "persistentvolumeclaims", "pvc"},
    "Role": {"role", "roles"},
    "RoleBinding": {"rolebinding", "rolebindings"},
    "Secret": {"secret", "secrets"},
    "Service": {"service", "services", "svc"},
    "ServiceAccount": {"serviceaccount", "serviceaccounts", "sa"},
    "StatefulSet": {"statefulset", "statefulsets"},
}

# Sanity check: we must have aliases for every supported resource kind.
assert set(SUPPORTED_KINDS) == set(RESOURCE_ALIASES.keys())

SUPPORTED_VERSIONS = ("1.9", "1.10", "1.11", "1.13", "1.14")

Filepath = pathlib.Path


# -----------------------------------------------------------------------------
#                                  Kubernetes
# -----------------------------------------------------------------------------
class K8sClientCert(NamedTuple):
    crt: Filepath = Filepath()
    key: Filepath = Filepath()


class K8sConfig(NamedTuple):
    """Everything we need to know to connect and authenticate with Kubernetes."""
    url: str = ""               # Kubernetes API
    token: str = ""             # Optional access token (eg Minikube).

    # Certificate authority credentials and self signed client certificate.
    # Used to authenticate to eg GKE.
    ca_cert: Filepath = Filepath()
    client_cert: Optional[K8sClientCert] = None

    # Kubernetes version and name.
    version: str = ""
    name: str = ""


class MetaManifest(NamedTuple):
    """A succinct summary of a K8s resource manifest."""
    apiVersion: str
    kind: str
    namespace: str
    name: str


# -----------------------------------------------------------------------------
#                                Deployment Plan
# -----------------------------------------------------------------------------
class JsonPatch(NamedTuple):
    """The URL for the patches as well as the patch payloads themselves."""
    # Send the patch to https://1.2.3.4/api/v1/namespace/foo/services
    url: str

    # The list of JSON patches.
    ops: Collection[str]


class DeltaCreate(NamedTuple):
    meta: MetaManifest
    url: str
    manifest: Dict[str, Any]


class DeltaDelete(NamedTuple):
    meta: MetaManifest
    url: str
    manifest: Dict[str, Any]


class DeltaPatch(NamedTuple):
    meta: MetaManifest
    diff: str
    patch: JsonPatch


class DeploymentPlan(NamedTuple):
    """Describe Square plan.

    Collects all resources manifests to add/delete as well as the JSON
    patches that make up a full plan.

    """
    create: Collection[DeltaCreate]
    patch: Collection[DeltaPatch]
    delete: Collection[DeltaDelete]


class DeploymentPlanMeta(NamedTuple):
    """Same as `DeploymentPlan` but contains `MetaManifests` only."""
    create: Collection[MetaManifest]
    patch: Collection[MetaManifest]
    delete: Collection[MetaManifest]


# -----------------------------------------------------------------------------
#                             Square Configuration
# -----------------------------------------------------------------------------
class Selectors(NamedTuple):
    """Comprises all the filters to select manifests."""
    kinds: Iterable[str]
    namespaces: Optional[Iterable[str]]
    labels: Optional[Set[Tuple[str, str]]]


class GroupBy(NamedTuple):
    """Define how to organise downloaded manifest on the files system."""
    label: str                  # "app"
    order: Iterable[str]        # ["ns", "label=app", kind"]


class Configuration(NamedTuple):
    """Square configuration.

    This will be compiled at startup but is immutable afterwards.

    """
    command: str
    folder: Filepath
    selectors: Selectors
    groupby: GroupBy
    kubeconfig: Filepath
    kube_ctx: Optional[str] = None
    verbosity: int = 0
    k8s_config: K8sConfig = K8sConfig()
    k8s_client: Any = None


# -----------------------------------------------------------------------------
#                                 Miscellaneous
# -----------------------------------------------------------------------------
LocalManifests = Dict[Filepath, Tuple[MetaManifest, dict]]
LocalManifestLists = Dict[Filepath, Collection[Tuple[MetaManifest, dict]]]
ServerManifests = Dict[MetaManifest, dict]
