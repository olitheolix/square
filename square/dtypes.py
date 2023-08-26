from pathlib import Path
from typing import Any, Callable, Dict, List, NamedTuple, Set, Tuple

import httpx
from pydantic import BaseModel, Field, field_validator
from typing_extensions import Annotated

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
class MetaManifest(NamedTuple):
    """Minimum amount of information to uniquely identify a K8s resource.

    The primary purpose of this tuple is to provide an immutable UUID that
    we can use as keys in dictionaries and sets.

    """
    apiVersion: str
    kind: str
    namespace: str | None

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
    # Kubernetes URL, version and name.
    url: str = ""
    name: str = ""
    version: str = ""

    # Bearer token (eg Minikube, KinD)
    token: str = ""

    # Certificate authority for self signed certificates.
    cadata: str | None = None
    cert: Tuple[Path, Path] | None = None
    headers: Dict[str, str] = {}

    # HttpX client to access the cluster. Will be replace with a properly
    # configured client in `k8s.create_httpx_client`.
    client: httpx.AsyncClient = httpx.AsyncClient()

    # Kubernetes API endpoints (see `k8s.compile_api_endpoints`).
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


# Workaround for a circular import with `callbacks`. The `callbacks` module
# needs the `Config` type annotation but `dtypes.Config` needs the callbacks.
# To break the cycle we use this dummy function as the default callback and
# install the proper callbacks during the validation phase of the `Config`
# ctor at runtime.
def do_nothing(): return        # codecov-skip


class Config(BaseModel):
    """Uniform interface into top level Square API."""
    # Path to local manifests eg "./foo"
    folder: Path

    # Path to Kubernetes credentials.
    kubeconfig: Path

    # Kubernetes context (use `None` to use the default).
    kubecontext: str | None = None

    # Only operate on resources that match the selectors.
    selectors: Selectors = Selectors()

    # Sort the manifest in this order, or alphabetically at the end if not in the list.
    priorities: List[str] = list(DEFAULT_PRIORITIES)

    # How to structure the folder directory when syncing manifests.
    groupby: GroupBy = GroupBy()

    # Define which fields to skip for which resource.
    filters: Filters = {}

    # Invoked for every local/server manifest that requires patching.
    patch_callback: Annotated[
        Callable,
        Field(validate_default=True)
    ] = do_nothing

    # Invoked for every manifest downloaded from cluster.
    strip_callback: Annotated[
        Callable,
        Field(validate_default=True)
    ] = do_nothing

    @field_validator('filters')
    @classmethod
    def validate_filters(cls, filters: Filters) -> Filters:
        # The top level filter structure must be a Dict that denotes a resource
        # type, eg `{"Deployment": [...], "Service": [...]}`.
        for k, v in filters.items():
            if k == "" or not isinstance(k, str):
                raise ValueError(f"Dict key <{k}> must be a non-empty string")
            validate_subfilters(v)
        return filters

    @field_validator("patch_callback")
    @classmethod
    def default_patch_callback(cls, cb: Callable) -> Callable:
        if cb == do_nothing:
            import square.callbacks
            return square.callbacks.patch_manifests
        return cb

    @field_validator("strip_callback")
    @classmethod
    def default_strip_callback(cls, cb: Callable) -> Callable:
        if cb == do_nothing:
            import square.callbacks
            return square.callbacks.strip_manifest
        return cb


def validate_subfilters(filter_list):
    """Recursively verify that every element in `filter_list` is valid."""
    if not isinstance(filter_list, list):
        raise ValueError(f"<{filter_list}> must be a list")

    for el in filter_list:
        if not isinstance(el, (dict, str)):
            raise ValueError(f"<{el}> must be a string or dict")

        if el == "":
            raise ValueError("Strings must be non-empty")

        # All dicts must contain exactly one non-empty key.
        if isinstance(el, dict):
            if "" in el or len(el) != 1:
                raise ValueError(f"<{el}> must have exactly one key")

            key = list(el)[0]
            if not isinstance(key, str):
                raise ValueError(f"Dict key <{key}> must be a string")

            # Recursively check the dictionary values.
            validate_subfilters(el[key])


# -----------------------------------------------------------------------------
#                                 Miscellaneous
# -----------------------------------------------------------------------------
LocalManifests = Dict[Path, Tuple[MetaManifest, dict]]
LocalManifestLists = Dict[Path, List[Tuple[MetaManifest, dict]]]
SquareManifests = Dict[MetaManifest, dict]
