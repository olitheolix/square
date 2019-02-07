import collections

# We support these resource types. The order matters because it determines the
# order in which the manifests will be grouped in the output files.
SUPPORTED_KINDS = (
    "Namespace", "ConfigMap", "Secret", "Service", "Deployment", "Ingress",
)
SUPPORTED_VERSIONS = ("1.9", "1.10")

DeltaCreate = collections.namedtuple("DeltaCreate", "meta url manifest")
DeltaDelete = collections.namedtuple("DeltaDelete", "meta url manifest")
DeltaPatch = collections.namedtuple("Delta", "meta diff patch")
DeploymentPlan = collections.namedtuple('DeploymentPlan', 'create patch delete')
JsonPatch = collections.namedtuple('Patch', 'url ops')
Manifests = collections.namedtuple('Manifests', 'local server files')
MetaManifest = collections.namedtuple('MetaManifest', 'apiVersion kind namespace name')
RetVal = collections.namedtuple('RetVal', 'data err')

RESOURCE_SCHEMA = {}
