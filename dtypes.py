import pathlib
from collections import namedtuple
from typing import Dict, List, Tuple, Union

# We support these resource types. The order matters because it determines the
# order in which the manifests will be grouped in the output files.
SUPPORTED_KINDS = (
    # Namespaces must come first to ensure the other resources can be created
    # within them.
    "Namespace",

    # Configuration and PVC before Deployments & friends use them.
    "ConfigMap", "Secret", "PersistentVolumeClaim",

    # RBAC.
    "ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding",

    # Define Services before creating Deployments & friends.
    "Service",

    # Everything that will spawn pods.
    "Deployment", "DaemonSet", "StatefulSet",

    # Ingresses should be after Deployments & friends.
    "Ingress",
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
    "ClusterRole": {"clusterrole", "clusterroles"},
    "ClusterRoleBinding": {"clusterrolebinding", "clusterrolebindings"},
    "Role": {"role", "roles"},
    "RoleBinding": {"rolebinding", "rolebindings"},
    "DaemonSet": {"daemonset", "daemonsets", "ds"},
    "StatefulSet": {"statefulset", "statefulsets"},
    "PersistentVolumeClaim": {"persistentVolumeClaim", "persistentvolumeclaims", "pvc"},
}

# Sanity check: we must have aliases for every supported resource kind.
assert set(SUPPORTED_KINDS) == set(RESOURCE_ALIASES.keys())

SUPPORTED_VERSIONS = ("1.9", "1.10", "1.11")

Config = namedtuple('Config', 'url token ca_cert client_cert version')
DeltaCreate = namedtuple("DeltaCreate", "meta url manifest")
DeltaDelete = namedtuple("DeltaDelete", "meta url manifest")
DeltaPatch = namedtuple("Delta", "meta diff patch")
DeploymentPlan = namedtuple('DeploymentPlan', 'create patch delete')
JsonPatch = namedtuple('Patch', 'url ops')
MetaManifest = namedtuple('MetaManifest', 'apiVersion kind namespace name')

# Data types.
Filepath = Union[str, pathlib.PurePath]
LocalManifests = Dict[Filepath, Tuple[MetaManifest, dict]]
LocalManifestLists = Dict[Filepath, List[Tuple[MetaManifest, dict]]]
ServerManifests = Dict[MetaManifest, dict]
