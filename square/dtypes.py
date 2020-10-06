import pathlib
from typing import (
    TYPE_CHECKING, Any, Collection, Dict, List, NamedTuple, Optional, Set,
    Tuple, Union,
)

if TYPE_CHECKING:
    from dataclasses import dataclass
else:
    import dataclasses

    from pydantic.dataclasses import dataclass

# All files in Square have this type.
Filepath = pathlib.Path

# Square will first save/deploy the resources in this list in this order.
# Afterwards it will move on to all those resources not in this list. The order
# in which it does that is undefined.
DEFAULT_PRIORITIES = (
    # Custom Resources should come first.
    "CustomResourceDefinition",

    # Commone non-namespaced resources.
    "ClusterRole", "ClusterRoleBinding",

    # Namespaces must come before any namespaced resources,
    "Namespace",

    # Configuration and PVC before Deployments & friends use them.
    "ConfigMap", "PersistentVolumeClaim", "Secret",

    # RBAC.
    "Role", "RoleBinding", "ServiceAccount",

    # Define Services before creating Deployments & friends.
    "PodDisruptionBudget", "Service",

    # Everything that will spawn pods.
    "CronJob", "DaemonSet", "Deployment", "StatefulSet",

    # Other.
    "HorizontalPodAutoscaler", "Ingress",
)


# -----------------------------------------------------------------------------
#                                  Kubernetes
# -----------------------------------------------------------------------------
class K8sClientCert(NamedTuple):
    crt: Filepath = Filepath()
    key: Filepath = Filepath()


class MetaManifest(NamedTuple):
    """Minimum amount of information to uniquely identify a K8s resource.

    The primary purpose of this tuple is to provide an immutable UUID that
    we can use as keys in dictionaries and sets.

    """
    apiVersion: str
    kind: str
    namespace: Optional[str]
    name: str


class K8sResource(NamedTuple):
    """Describe a specific K8s resource kind."""
    apiVersion: str   # "batch/v1beta1" or "extensions/v1beta1".
    kind: str         # "Deployment" (as specified in manifest)
    name: str         # "deployment" (usually lower case version of above)
    namespaced: bool  # Whether or not the resource is namespaced.
    url: str          # API endpoint, eg "k8s-host.com/api/v1/pods".


class K8sConfig(NamedTuple):
    """Everything we need to know to connect and authenticate with Kubernetes."""
    url: str = ""               # Kubernetes API
    token: str = ""             # Optional access token (eg Minikube).

    # Certificate authority credentials and self signed client certificate.
    # Used to authenticate to eg GKE.
    ca_cert: Filepath = Filepath()
    client_cert: Optional[K8sClientCert] = None

    # Request session.
    client: Any = None

    # Kubernetes version and name.
    version: str = ""
    name: str = ""

    # Kubernetes API endpoints (see k8s.compile_api_endpoints).
    apis: Dict[Tuple[str, str], K8sResource] = {}

    # LUT to translate short names into their proper resource kind,
    # for instance short = {"service":, "Service", "svc": "Service"}
    short2kind: Dict[str, str] = {}

    # The set of supported K8s resource kinds, eg {"Deployment", "Service"}.
    # NOTE: these are the `manifest.kind` spellings. "Deployment" is a valid
    # K8s resource kind, whereas "deployment" or "Deployments" are not.
    kinds: Set[str] = set()


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
def _factory(ret):
    return dataclasses.field(default_factory=lambda: ret)


@dataclass
class Selectors:
    """Comprises all the filters to select manifests."""
    kinds: Set[str] = _factory(set(DEFAULT_PRIORITIES))
    namespaces: List[str] = _factory([])
    labels: List[str] = _factory([])


@dataclass
class GroupBy:
    """Define how to organise downloaded manifest on the files system."""
    label: str = ""                  # "app"
    order: List[str] = _factory([])  # ["ns", "label=app", kind"]


@dataclass
class Config:
    """Uniform interface into top level Square API."""
    # Path to local manifests eg "./foo"
    folder: Filepath

    # Path to Kubernetes credentials.
    kubeconfig: Filepath

    # Kubernetes context (use `None` to use the default).
    kubecontext: Optional[str]

    # Only operate on resources that match the selectors.
    selectors: Selectors = Selectors()

    # Sort the manifest in this order, or alphabetically at the end if not in the list.
    priorities: List[str] = _factory(list(DEFAULT_PRIORITIES))

    # How to structure the folder directory when syncing manifests.
    groupby: GroupBy = GroupBy()

    # Define which fields to skip for which resource.
    filters: Dict[str, List[Union[str, dict]]] = _factory({})

    version: str = ""


# -----------------------------------------------------------------------------
#                                 Miscellaneous
# -----------------------------------------------------------------------------
LocalManifests = Dict[Filepath, Tuple[MetaManifest, dict]]
LocalManifestLists = Dict[Filepath, Collection[Tuple[MetaManifest, dict]]]
ServerManifests = Dict[MetaManifest, dict]
