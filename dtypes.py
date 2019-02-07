from collections import namedtuple

# We support these resource types. The order matters because it determines the
# order in which the manifests will be grouped in the output files.
SUPPORTED_KINDS = (
    "Namespace", "ConfigMap", "Secret", "Service", "Deployment", "Ingress",
)
SUPPORTED_VERSIONS = ("1.9", "1.10")

Config = namedtuple('Config', 'url token ca_cert client_cert version')
DeltaCreate = namedtuple("DeltaCreate", "meta url manifest")
DeltaDelete = namedtuple("DeltaDelete", "meta url manifest")
DeltaPatch = namedtuple("Delta", "meta diff patch")
DeploymentPlan = namedtuple('DeploymentPlan', 'create patch delete')
JsonPatch = namedtuple('Patch', 'url ops')
Manifests = namedtuple('Manifests', 'local server files')
MetaManifest = namedtuple('MetaManifest', 'apiVersion kind namespace name')
RetVal = namedtuple('RetVal', 'data err')
