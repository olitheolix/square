"""Define which manifest fields to exclude from diffs and patches.

For instance, almost all manifests can have a "status" field or a
`manifest.uid` that K8s manages itself. It does not make sense, or is even
dangerous, for Square to compute diffs on such field or patch them.

This module defines utility functions to define these filters.

"""
import logging
from typing import List, Union

# Convenience.
logit = logging.getLogger("square")


def valid(filters: List[Union[dict, list, str]]) -> bool:
    """Return `True` iff `filters` is valid."""
    if not isinstance(filters, list):
        logit.error(f"<{filters}> must be a list")
        return False

    # Iterate over all fields of all K8s resource type.
    for el in filters:
        # All filterss must contain only dicts and boolean `False` values.
        if isinstance(el, dict):
            if len(el) != 1:
                logit.error(f"<{el}> must have exactly one key")
                return False
            value = list(el.values())[0]

            # Recursively check the dictionary values.
            if not valid(value):
                logit.error(f"<{value}> is invalid")
                return False
        elif isinstance(el, str):
            if el == "":
                logit.error("Strings must be non-empty")
                return False
        else:
            logit.error(f"<{el}> must be a string")
            return False
    return True


def default():
    """Return a default set of filters that should be applicable to all resources."""
    return [
        {"metadata": [
            {"annotations": [
                "deployment.kubernetes.io/revision",
                "kubectl.kubernetes.io/last-applied-configuration",
                "kubernetes.io/change-cause",
            ]},
            "creationTimestamp",
            "generation",
            "resourceVersion",
            "selfLink",
            "uid",
        ]},
        "status",
    ]
