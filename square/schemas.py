"""Define which manifest fields to exclude from diffs and patches.

For instance, almost all manifests can have a "status" field or a
`manifest.uid` that K8s manages itself. It does not make sense, or is even
dangerous, for Square to compute diffs on such field or patch them.

This module defines utility functions to define these filters.

"""
import copy
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


def merge(src: list, dst: list) -> list:
    """Merge `src` into `dst` and return a copy of `dst`."""
    # Avoid side effects.
    dst = copy.deepcopy(dst)

    def find(data: list, key: str) -> dict:
        for el in data:
            if isinstance(el, dict) and set(el.keys()) == {key}:
                return el
        assert False, "BUG"

    def _update(src, dst):
        # Add all string keys from `src` to `dst` if they are not yet in `dst`.
        str_keys = [_ for _ in src if isinstance(_, str) and _ not in dst]
        dst.extend(str_keys)

        # Find the all dicts and their one and only key.
        dict_src = {tuple(_.keys())[0] for _ in src if isinstance(_, dict)}
        dict_dst = {tuple(_.keys())[0] for _ in dst if isinstance(_, dict)}

        # Recursively merge the dictionaries.
        for key in dict_src:
            if key not in dict_dst:
                # `dst` does not have the dict at all - just copy it.
                dst.append(find(src, key))
            else:
                # `dst` already has a dict for `key` -> recursively update it.
                src_val = find(src, key)[key]
                dst_val = find(dst, key)[key]
                _update(src_val, dst_val)

        # Sort the list alphabetically (if the entry is a dict then use its one
        # and only key as the comparative element).
        dst.sort(key=lambda _: _ if isinstance(_, str) else tuple(_.keys())[0])

    _update(src, dst)
    return dst
