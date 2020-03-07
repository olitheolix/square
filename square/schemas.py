"""Define the structure of the stripped manifest schema.

A stripped manifest is a sub-set of a normal manifest. The remaining keys
capture the salient information about the resource.

For instance, almost all manifest can have a "status" field. Albeit useful for
diagnostics, it makes no sense to compute diffs of "status" fields and submit
them in patches.

# Schema Conventions
Schemas are normal dictionaries without a depth limit.  All keys correspond to
a K8s manifest key. All value must be either dicts themselves, a bool to
specify whether the fields must be included in the stripped manifest, or None
if the field is not mandatory but should be included.

* True: field will be included. It is an error if the input manifest lacks it.
* False: field will not be included. It is an error if the input manifest has it.
* None: field will be included if the input manifest has it, and ignored otherwise.

"""
import logging

from typing import Any, Dict, Union

# Convenience.
logit = logging.getLogger("square")


schema_1_9: Dict[str, Dict[Any, Any]] = {
    "ClusterRole": {},
    "ClusterRoleBinding": {},
    "ConfigMap": {},
    "CronJob": {},
    "DaemonSet": {},
    "Deployment": {
        "status": False,
    },
    "HorizontalPodAutoscaler": {},
    "Ingress": {},
    "Namespace": {},
    "PersistentVolumeClaim": {},
    "Role": {},
    "RoleBinding": {},
    "Secret": {},
    "Service": {},
    "ServiceAccount": {},
    "StatefulSet": {},
}


EXCLUSION_SCHEMA: Dict[str, Dict[str, Dict[Any, Any]]] = {
    "1.9": schema_1_9,
    "1.10": schema_1_9,
    "1.11": schema_1_9,
    "1.13": schema_1_9,
    "1.14": schema_1_9,
}


def _is_exclusion_sane(schema: Dict[str, Union[dict, bool]]) -> bool:
    """Return `True` iff `schema` is valid."""
    # Iterate over all fields of all K8s resource type.
    for k, v in schema.items():
        assert isinstance(k, str)

        # All schema can only contains dicts and boolean `False` values.
        if isinstance(v, dict):
            # Recursively check the dict to ensure it also only contains dicts
            # and boolean `False` values.
            if not _is_exclusion_sane(v):
                logit.error(f"<{v}> is invalid")
                return False
        elif v is False:
            # Boolean `False` is what we expect at the leaf.
            pass
        else:
            logit.error(f"<{k}> is not a boolean `False`")
            return False
    return True


def populate_schemas(schemas: Dict[str, Dict[str, Dict[Any, Any]]]) -> bool:
    """Add default values to all exclusion schemas and validate them."""
    # Iterate over all schemas and insert default values.
    for version, resource in schemas.items():
        for data in resource.values():
            if "metadata" not in data:
                data["metadata"] = {}
            if "annotations" not in data["metadata"]:
                data["metadata"]["annotations"] = {}

            data["metadata"].update({
                    "creationTimestamp": False,
                    "resourceVersion": False,
                    "selfLink": False,
                    "uid": False,
            })

            data["metadata"]["annotations"].update(
                {
                    "deployment.kubernetes.io/revision": False,
                    "kubectl.kubernetes.io/last-applied-configuration": False,
                }
            )

            # Ensure the exclusion schema is valid.
            if not _is_exclusion_sane(data):
                logit.error(f"ERROR - Exclusion schema {version} is invalid - abort")
                return True
    return False


# Finalise the exclusion schemas and sanity check them.
assert populate_schemas(EXCLUSION_SCHEMA) is False
