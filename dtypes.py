from collections import namedtuple

# We support these resource types. The order matters because it determines the
# order in which the manifests will be grouped in the output files.
SUPPORTED_KINDS = (
    "Namespace", "ConfigMap", "Secret", "Service", "Deployment", "Ingress",
)

# Declare aliases for each resource type. Will be used in command line parsing
# to save the user some typing and match what `kubectl` would accept. We do not
# need to worry about capitalisation because `square.parse_commandline_args`
# will always convert everything to lower case letters first.
RESOURCE_ALIASES = {
    "Namespace": {"namespace", "namespaces", "ns"},
    "ConfigMap": {"configmap", "cm"},
    "Ingress": {"ingress", "ingresses", "ing"},
    "Secret": {"secret", "secrets"},
    "Service": {"service", "services", "svc"},
    "Deployment": {"deployment", "deployments", "deploy"},
}
# Sanity check: we must have aliases for every supported resource kind.
assert set(SUPPORTED_KINDS) == set(RESOURCE_ALIASES.keys())

SUPPORTED_VERSIONS = ("1.9", "1.10")

Config = namedtuple('Config', 'url token ca_cert client_cert version')
DeltaCreate = namedtuple("DeltaCreate", "meta url manifest")
DeltaDelete = namedtuple("DeltaDelete", "meta url manifest")
DeltaPatch = namedtuple("Delta", "meta diff patch")
DeploymentPlan = namedtuple('DeploymentPlan', 'create patch delete')
JsonPatch = namedtuple('Patch', 'url ops')
Manifests = namedtuple('Manifests', 'meta files')
MetaManifest = namedtuple('MetaManifest', 'apiVersion kind namespace name')
RetVal = namedtuple('RetVal', 'data err')
