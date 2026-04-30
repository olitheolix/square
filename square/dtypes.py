from functools import cache
from pathlib import Path
from typing import Any, Callable, Dict, List, NamedTuple, Set, Tuple

import jsonpath_ng as jp
from jsonpath_ng.exceptions import JsonPathParserError
import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing_extensions import Annotated

# Square will first save/deploy the resources in this list in this order.
# Afterwards it will move on to all those resources not in this list. The order
# in which it does that is undefined.
DEFAULT_PRIORITIES = (
    # Custom Resources should come first.
    "customresourcedefinition",
    # Common non-namespaced resources.
    "clusterrole",
    "clusterrolebinding",
    # Namespaces must come before any namespaced resources.
    "namespace",
    # RBAC.
    "role",
    "rolebinding",
    "serviceaccount",
    # Everything else.
    "configmap",
    "service",
    "deployment.apps",
    "horizontalpodautoscaler.autoscaling",
    "ingress.networking.k8s.io",
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

    def skgn(self) -> "SelKindGroupNames":
        """Return the MetaManifest as a `SelKindGroupNames`."""
        ns = self.namespace if self.namespace else ""
        kind = self.kind.lower()
        gv = self.apiVersion.partition("/")[0]

        value = f"{kind}.{gv}" if gv else kind
        if self.name:
            value += f"/{self.name}"
        return SelKindGroupNames(value=value, ns=ns)


class K8sResource(NamedTuple):
    """Describe a specific K8s resource kind."""

    apiVersion: str  # "batch/v1beta1" or "extensions/v1beta1".
    kind: str  # "Deployment" (as specified in manifest).
    name: str  # "deployments" (plural name, lower case).
    namespaced: bool  # Whether or not the resource is namespaced.
    url: str  # API endpoint, eg "k8s-host.com/api/v1/pods".
    aliases: Tuple[str, ...]  # all names (singular, plural, short hands).
    preferred: bool = False


class SelKindGroupNames(BaseModel):
    """Square internal format to store Kind, Group, Version, Namespace and Name.

    This is similar to `MetaManifest` but tailored specifically to how users
    can target specific resources with Square. In particular, users can specify
    "pod", or "pod.v1", or "PoD", or "POD/name" etc in the configuration file
    or on the command line. These strings are devoid of the specific API
    version and ignore capitalisation. This improves the UX. Square also
    contains logic to pick the preferred API version automatically.

    NOTE: every `MetaManifest` can be converted to a `SelKindGroupName`, but the
    reverse is not true.

    """

    value: str
    ns: str = ""

    def __str__(self):
        kg = self.kind_group
        return f"{kg}/{self.name}" if self.name else kg

    @field_validator("value")
    def validate_kind_group_names(cls, v):
        if not v:
            raise ValueError("String must not be empty")

        v = v.lower()
        if v.strip() != v:
            raise ValueError(f"value contains white space <{v}>")

        if v.startswith("/"):
            raise ValueError(f"value has no kind <{v}>")

        # pod.v1/name -> ["pod.v1", "name"]
        parts = v.split("/")
        if len(parts) > 2 or v.endswith("/"):
            raise ValueError(f'At most one "/" is allowed <{v}>')

        for part in parts:
            if part.strip() != part:
                raise ValueError(f"value contains white space <{v}>")

        return v

    @property
    def kind(self) -> str:
        # Extract the kind and group from the `value` string.
        # Example: pod.v1/name -> "pod"
        kind_name = self.value.partition(".")[0]
        kind = kind_name.partition("/")[0]
        return kind

    @property
    def group(self) -> str:
        # Extract the kind and group from the `value` string.
        # Example: deploy.apps/v1 -> "apps"
        group_name = self.value.partition(".")[2]
        group = group_name.partition("/")[0]
        return group

    @property
    def name(self) -> str:
        # Extract the resource name from the `value` string.
        # Example: pod.v1/name -> "name"
        if "/" in self.value:
            return self.value.split("/", 1)[1]
        return ""

    @property
    def kind_group(self) -> str:
        # Extract the kind and group from the `value` string.
        # Example: pod.v1/name -> "pod.v1"
        return f"{self.kind}.{self.group}" if self.group else self.kind

    @property
    def namespace(self) -> str:
        return self.ns


class K8sConfig(BaseModel):
    """Everything we need to know to connect and authenticate with Kubernetes."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

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

    # HttpX client to access the cluster. The `k8s.create_httpx_client` will
    # replace it with a properly configured client.
    client: httpx.AsyncClient = httpx.AsyncClient()

    # Kubernetes API endpoints (see `k8s.compile_api_endpoints`).
    apis: Dict[str, List[K8sResource]] = {}


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
class Selectors(BaseModel):
    """Parameters to target specific groups of manifests."""

    model_config = {"str_strip_whitespace": True}

    kinds: Set[str] = set()
    namespaces: List[str] = []
    labels: List[str] = []

    @property
    def str_skgns(self) -> Set[str]:
        """Set of all stringified kind/group/name information."""
        return {str(SelKindGroupNames(value=_)) for _ in self.kinds}


class GroupBy(BaseModel):
    """Define how to organise downloaded manifests on the files system."""

    label: str = ""  # "app"
    order: List[str] = []  # ["ns", "label=app", kind"]


class ConnectionParameters(BaseModel):
    """Define HttpX specific connection parameters."""

    # Extra headers to pass along to the Kubernetes API.
    k8s_extra_headers: Dict[str, str] = dict()

    # Disable strict SSL certificate checks for cluster. This is only
    # recommended for old clusters that were years ago. See this link for more
    # info: https://github.com/aws/containers-roadmap/issues/2638
    disable_x509_strict: bool = False

    # https://www.python-httpx.org/advanced/#timeout-configuration
    connect: float = 5
    read: float = 5
    write: float = 5
    pool: float = 5

    # https://www.python-httpx.org/advanced/#pool-limit-configuration
    max_connections: int | None = None
    max_keepalive_connections: int | None = None
    keepalive_expiry: float = 5.0

    # Enable transport protocols.
    http1: bool = True
    http2: bool = True


"""Define the new-style filters using JSON path strings."""
Filters = Dict[str, List[str]]  # eg {"Deployment": [".spec.replicas"]}


# Workaround for a circular import with `callbacks`. The `callbacks` module
# needs the `Config` type annotation but `dtypes.Config` needs the callbacks.
# To break the cycle we use this dummy function as the default callback and
# install the proper callbacks during the validation phase of the `Config`
# ctor at runtime.
def do_nothing():
    return  # codecov-skip


@cache
def jpcache(p: str) -> jp.JSONPath:
    """Parse a JSONPath expression and cache the result."""
    return jp.parse(p)


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
    # Examples: ["pod", "service.v1", "deploy.apps"]
    priorities: List[str] = list(DEFAULT_PRIORITIES)

    # How to structure the folder directory when syncing manifests.
    groupby: GroupBy = GroupBy()

    # Define which fields to skip for which resource (new JSON path format).
    filters: Filters = Field(default_factory=dict)

    # Connection timeouts, headers and extra SSL configurations.
    connection_parameters: ConnectionParameters = ConnectionParameters()

    # Square will not touch this. Useful to pass extra information to callbacks.
    user_data: Any = None

    # Invoked for every local/server manifest that requires patching.
    patch_callback: Annotated[Callable, Field(validate_default=True)] = do_nothing

    # Invoked for every manifest downloaded from cluster.
    strip_callback: Annotated[Callable, Field(validate_default=True)] = do_nothing

    class Config:
        # Pydantic config: ensure that all fields are validated on assignment,
        # not just at initialisation.
        validate_assignment = True
        arbitrary_types_allowed = True

    @field_validator("filters")
    @classmethod
    def validate_filters(cls, filters: Filters) -> Filters:
        """Ensure the keys denote valid resource types and the values are valid
        JSON paths.

        Returns the original `filters` dict with all the keys lowercased for
        easier matching and consistency later on.

        """
        # External representation accepts JSONPath expressions as strings, but the
        # internal representation stores only compiled `jp.JSONPath` objects.
        #
        # Compilation occurs during validation so that runtime access operates solely
        # on pre-parsed expressions, avoiding repeated parsing overhead.
        out = {k.lower(): v for k, v in filters.items()}
        for key, paths in out.items():
            # Run Pydantic validation on the key (eg "Deployment").
            SelKindGroupNames(value=key)

            try:
                for path in paths:
                    jpcache(path)
            except JsonPathParserError:
                raise ValueError(f"invalid JSON path <{path}>")

        return out

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


# -----------------------------------------------------------------------------
#                                 Miscellaneous
# -----------------------------------------------------------------------------
LocalManifests = Dict[Path, Tuple[MetaManifest, dict]]
LocalManifestLists = Dict[Path, List[Tuple[MetaManifest, dict]]]
SquareManifests = Dict[MetaManifest, dict]
