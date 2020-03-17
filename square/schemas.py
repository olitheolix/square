"""Define which manifest fields to exclude from diffs and patches.

For instance, almost all manifests can have a "status" field that K8s will
populate and update as necessary. It does not make sense for Square to compute
diffs on that field or patch them. Therefore, we add it to the exclusion filter
defined here.

"""
import logging
from typing import Any, Dict, Union

# Convenience.
logit = logging.getLogger("square")


EXCLUSION_SCHEMA: Dict[str, dict] = {
    "ClusterRole": {},
    "ClusterRoleBinding": {},
    "ConfigMap": {
        "metadata": {"annotations": {
            "control-plane.alpha.kubernetes.io/leader": False,
        }}
    },
    "CronJob": {},
    "DaemonSet": {},
    "Deployment": {},
    "HorizontalPodAutoscaler": {
        "metadata": {"annotations": {
            "control-plane.alpha.kubernetes.io/leader": False,
            "autoscaling.alpha.kubernetes.io/conditions": False,
            "autoscaling.alpha.kubernetes.io/current-metrics": False,
        }}
    },
    "Ingress": {},
    "Namespace": {},
    "PersistentVolumeClaim": {},
    "Role": {},
    "RoleBinding": {},
    "Secret": {},
    "Service": {"spec": {"clusterIP": False, "sessionAffinity": False}},
    "ServiceAccount": {},
    "StatefulSet": {},
}


def _is_exclusion_sane(schema: Dict[str, Union[dict, bool]]) -> bool:
    """Return `True` iff `schema` is valid."""
    # Iterate over all fields of all K8s resource type.
    for k, v in schema.items():
        assert isinstance(k, str)

        # All schemas must contain only dicts and boolean `False` values.
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


def populate_schemas(schemas: Dict[str, Dict[Any, Any]]) -> bool:
    """Add default values to all exclusion schemas and validate them."""
    # Iterate over all schemas and insert default values.
    for resource_kind, data in schemas.items():
        # Ensure that `metadata.annotations` exists.
        data["metadata"] = data.get("metadata", {})
        data["metadata"]["annotations"] = data["metadata"].get("annotations", {})

        # We do not want to manage the status of a resource since K8s
        # updates that whenever necessary with the latest values.
        data["status"] = False

        # Default resource tags that K8s manages itself. It would be
        # dangerous to overwrite them.
        data["metadata"].update({
            "creationTimestamp": False,
            "generation": False,
            "resourceVersion": False,
            "selfLink": False,
            "uid": False,
        })

        # Never touch the annotation of `kubectl`.
        data["metadata"]["annotations"].update(
            {
                "deployment.kubernetes.io/revision": False,
                "kubectl.kubernetes.io/last-applied-configuration": False,
                "kubernetes.io/change-cause": False,
            }
        )

        # Ensure the exclusion schema is valid.
        if not _is_exclusion_sane(data):
            logit.error(f"ERROR - Exclusion schema for <{resource_kind}> is invalid")
            return True
    return False


# Finalise the exclusion schemas and sanity check them.
assert populate_schemas(EXCLUSION_SCHEMA) is False
