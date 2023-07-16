"""Define which manifest fields to exclude from diffs and patches.

For instance, almost all manifests can have a "status" field or a
`manifest.uid` that K8s manages itself. It does not make sense, or is even
dangerous, for Square to compute diffs on such field or patch them.

This module defines utility functions to define these filters.

"""
import copy
import logging
from types import SimpleNamespace
from typing import Tuple

import pydantic
import yaml

from square.dtypes import Config, Filepath

from .dtypes import FiltersKind

# Convenience.
logit = logging.getLogger("square")


def valid(filters: FiltersKind) -> bool:
    """Return `True` iff `filters` is valid."""
    return _valid(filters)


def _valid(filters) -> bool:
    """Return `True` iff `filters` is valid."""
    if not isinstance(filters, list):
        logit.error(f"<{filters}> must be a list")
        return False

    # Iterate over all fields of all K8s resource type.
    for el in filters:
        # All entries must be either strings or dicts of lists.
        if isinstance(el, dict):
            if len(el) != 1:
                logit.error(f"<{el}> must have exactly one key")
                return False
            value = list(el.values())[0]

            # Recursively check the dictionary values.
            if not _valid(value):
                logit.error(f"<{value}> is invalid")
                return False
        elif isinstance(el, str):
            if el == "":
                logit.error("Strings must be non-empty")
                return False
        else:
            logit.error(f"<{el}> must be a string or dict")
            return False
    return True


def merge(src: list, dst: list) -> list:
    """Merge `src` into `dst` and return a copy of `dst`."""
    # Avoid side effects.
    dst = copy.deepcopy(dst)

    def find_dict(data: list, key: str) -> dict:
        """Find and return the dictionary in `data` that has the `key`."""
        tmp = [_ for _ in data if isinstance(_, dict) and set(_.keys()) == {key}]
        assert len(tmp) == 1
        return tmp[0]

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
                dst.append(find_dict(src, key))
            else:
                # `dst` already has a dict for `key` -> recursively update it.
                src_val = find_dict(src, key)[key]
                dst_val = find_dict(dst, key)[key]
                _update(src_val, dst_val)

        # Sort the list alphabetically (if the entry is a dict then use its one
        # and only key as the comparative element).
        dst.sort(key=lambda _: _ if isinstance(_, str) else tuple(_.keys())[0])

    _update(src, dst)
    return dst


def load(fname: Filepath) -> Tuple[Config, bool]:
    """Parse the Square configuration file `fname` and return it as a `Config`."""
    err_resp = Config(folder=Filepath(""), kubeconfig=Filepath("")), True
    fname = Filepath(fname)

    # Load the configuration file.
    try:
        raw = yaml.safe_load(Filepath(fname).read_text())
    except FileNotFoundError as e:
        logit.error(f"Cannot load config file <{fname}>: {e.args[1]}")
        return err_resp
    except yaml.YAMLError as exc:
        msg = f"Could not parse YAML file {fname}"

        # Special case: parser supplied location information.
        mark = getattr(exc, "problem_mark", SimpleNamespace(line=-1, column=-1))
        line, col = (mark.line + 1, mark.column + 1)
        msg = f"YAML format error in {fname}: Line {line} Column {col}"
        logit.error(msg)
        return err_resp

    # Parse the configuration into `ConfigFile` structure.
    try:
        cfg = Config.model_validate(raw)

        # Explicitly access the computed `_kinds_names` attribute because
        # Pydantic will not compute it until it is accessed.
        cfg.selectors._kinds_names
        cfg.selectors._kinds_only
    except (pydantic.ValidationError, TypeError) as e:
        logit.error(f"Schema is invalid: {e}")
        return err_resp

    # Remove the "_common_" filter and merge it into all the other filters.
    common = cfg.filters.pop("_common_", [])
    cfg.filters = {k: merge(common, v) for k, v in cfg.filters.items()}
    cfg.filters["_common_"] = common

    # Ensure the path is an absolute path.
    cfg.folder = fname.parent.absolute() / cfg.folder

    return cfg, False
