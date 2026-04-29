"""Define which manifest fields to exclude from diffs and patches.

For instance, almost all manifests can have a "status" field or a
`manifest.uid` that K8s manages itself. It does not make sense, or is even
dangerous, for Square to compute diffs on such field or patch them.

This module defines utility functions to define these filters.

"""

import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Tuple

import pydantic
import yaml

from square.dtypes import Config

# Convenience.
logit = logging.getLogger("square")


def load(fname: Path) -> Tuple[Config, bool]:
    """Parse the Square configuration file `fname` and return it as a `Config`."""
    err_resp = Config(folder=Path(), kubeconfig=Path()), True

    # Load the configuration file.
    try:
        raw = yaml.safe_load(fname.read_text())
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

    # Parse the configuration into `Config` structure.
    try:
        cfg = Config.model_validate(raw)

        # Explicitly access the computed attributes since Pydantic will not
        # create and validate them until accessed, and we want all the error
        # checking to happen right now when we load the file.
        cfg.selectors.str_skgns
    except (pydantic.ValidationError, TypeError) as e:
        logit.error(f"Schema is invalid: {e}")
        return err_resp

    # Ensure the path is absolute.
    cfg.folder = fname.parent.absolute() / cfg.folder

    return cfg, False
