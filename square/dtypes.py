import pathlib
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Set, Tuple

from pydantic import BaseModel, Field

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

    # Every resource must have a name except for Namespaces, which encode their
    # name in the `namespace` field.
    name: str | None


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

    # A persistent session from the "requests" library for this cluster.
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
    ops: List[Dict[str, str]]


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
    create: List[DeltaCreate] | Tuple[DeltaCreate, ...]
    patch: List[DeltaPatch] | Tuple[DeltaPatch, ...]
    delete: List[DeltaDelete] | Tuple[DeltaDelete, ...]


class DeploymentPlanMeta(NamedTuple):
    """Same as `DeploymentPlan` but contains `MetaManifests` only."""
    create: List[MetaManifest] | Tuple[MetaManifest, ...]
    patch: List[MetaManifest] | Tuple[MetaManifest, ...]
    delete: List[MetaManifest] | Tuple[MetaManifest, ...]


# -----------------------------------------------------------------------------
#                             Square Configuration
# -----------------------------------------------------------------------------
class KindName(BaseModel):
    """Holds a K8s resource Kind."""
    kind: str = Field(min_length=1)  # Eg 'Service'
    name: str                        # Eg. 'appname'


class Selectors(BaseModel):
    """Parameters to target specific groups of manifests."""
    model_config = {"str_strip_whitespace": True}

    kinds: Set[str] = set()
    namespaces: List[str] = []
    labels: List[str] = []

    @property
    def _kinds_names(self) -> List[KindName]:
        """Deconstruct the Selector Kinds into Kind and Name.

        These are the valid scenarios:
          'Service'     -> ('Service', '')
          'Service/'    -> ('Service', '')
          'Service/foo' -> ('Service', 'foo')

        """
        # Unpack the 'Kind/Name' into a dedicated `KindName` model.
        ans: List[KindName] = []
        for src_kind in sorted(self.kinds):
            # Ensure the `src_kind` has at most one '/'.
            parts = src_kind.split("/")
            if len(parts) == 1:
                kind, name = parts[0], ""
            elif len(parts) == 2:
                kind, name = parts
            else:
                raise ValueError(f"Invalid kind {src_kind}")

            # There must be no leading/trailing white space.
            if (kind.strip() != kind) or (name.strip() != name):
                raise ValueError(f"Invalid kind {src_kind}")

            # Add the deconstructed Kind/Name selector.
            ans.append(KindName(kind=kind, name=name))
        return ans

    @property
    def _kinds_only(self) -> Set[str]:
        """Compile the set of resource KINDS we are interested in.

        NOTE: If the user selects `{"pod/app1", "deploy", "deploy/app1"}` then
        we only want to retain `deploy` since `pod/app1` selects a unique Pod
        whereas `deploy` selects all `Deployment` resources.

        """
        return {_.kind for _ in self._kinds_names if _.name == ""}


class GroupBy(BaseModel):
    """Define how to organise downloaded manifests on the files system."""
    label: str = ""                  # "app"
    order: List[str] = []            # ["ns", "label=app", kind"]


"""Define the filters to exclude sections of manifests."""
FiltersKind = List[str | dict]
Filters = Dict[str, FiltersKind]


class Config(BaseModel):
    """Uniform interface into top level Square API."""
    # Path to local manifests eg "./foo"
    folder: Filepath

    # Path to Kubernetes credentials.
    kubeconfig: Filepath

    # Kubernetes context (use `None` to use the default).
    kubecontext: Optional[str] = None

    # Only operate on resources that match the selectors.
    selectors: Selectors = Selectors()

    # Sort the manifest in this order, or alphabetically at the end if not in the list.
    priorities: List[str] = list(DEFAULT_PRIORITIES)

    # How to structure the folder directory when syncing manifests.
    groupby: GroupBy = GroupBy()

    # Define which fields to skip for which resource.
    filters: Filters = {}

    # Callable: will be invoked for every local/server manifest that requires
    # patching before the actual patch will be computed.
    patch_callback: Optional[Callable] = None

    version: str = ""


# -----------------------------------------------------------------------------
#                                 Miscellaneous
# -----------------------------------------------------------------------------
LocalManifests = Dict[Filepath, Tuple[MetaManifest, dict]]
LocalManifestLists = Dict[Filepath, List[Tuple[MetaManifest, dict]]]
SquareManifests = Dict[MetaManifest, dict]
