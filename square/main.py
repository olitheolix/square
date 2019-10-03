import argparse
import logging
import os
import pathlib
import re
import sys
import textwrap
from typing import Optional, Tuple

import square
import square.square
from square import __version__
from square.dtypes import (
    RESOURCE_ALIASES, SUPPORTED_KINDS, Configuration, ManifestHierarchy,
    Selectors,
)

# Convenience: global logger instance to avoid repetitive code.
logit = logging.getLogger("square")


def parse_commandline_args():
    """Return parsed command line."""
    name = os.path.basename(__file__)
    description = textwrap.dedent(f'''
    Manage Kubernetes manifests.

    Examples:
      {name} get
      {name} plan
      {name} apply
    ''')

    def _validate_kind(kind: str) -> str:
        """Convert resource `kind` from aliases to canonical name.
        For instance, `svc` -> `Service`.
        """
        kind = kind.lower()

        # The "all" resource is special - do not expand.
        if kind == "all":
            return "all"

        out = [
            canonical for canonical, aliases in RESOURCE_ALIASES.items()
            if kind in aliases
        ]

        # Must have found at most one or there is a serious bug.
        if len(out) != 1:
            raise argparse.ArgumentTypeError(kind)
        return out[0]

    def _validate_label(label: str) -> Tuple[str, ...]:
        """Convert resource `kind` from aliases to canonical name.
        For instance, `svc` -> `Service`.
        """
        pat = re.compile(r"^[a-z0-9][-a-z0-9_.]*=[-A-Za-z0-9_.]*[A-Za-z0-9]$")
        if pat.match(label) is None:
            raise argparse.ArgumentTypeError(label)
        return tuple(label.split("="))

    # A dummy top level parser that will become the parent for all sub-parsers
    # to share all its arguments.
    parent = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False
    )
    parent.add_argument(
        "-v", "--verbosity", action="count", default=0,
        help="Specify multiple times to increase log level."
             " -v: WARNING -vv: INFO -vvv: DEBUG"
    )
    parent.add_argument(
        "-n", type=str, nargs="*",
        metavar="ns", dest="namespaces",
        help="List of namespaces (omit to consider all)",
    )
    parent.add_argument(
        "-l", type=_validate_label, nargs="*",
        metavar="labels", dest="labels", default=tuple(),
        help="List of K8s resources to consider or just 'all'",
    )
    parent.add_argument(
        "--kubeconfig", type=str, metavar="path",
        default=os.environ.get("KUBECONFIG", None),
        help="Location of kubeconfig file (defaults to env KUBECONFIG)",
    )
    parent.add_argument(
        "--folder", type=str, metavar="path",
        default=os.environ.get("SQUARE_FOLDER", "./"),
        help="Manifest folder (defaults to env SQUARE_FOLDER)",
    )
    parent.add_argument(
        "--context", type=str, metavar="ctx", dest="ctx", default=None,
        help="Kubernetes context (use default one if unspecified)",
    )

    # The primary parser for the top level options (eg GET, PATCH, ...).
    parser = argparse.ArgumentParser(add_help=True)
    subparsers = parser.add_subparsers(
        help='Mode', dest='parser', metavar="ACTION",
        title="Operation", required=True
    )

    # Configuration for `kinds` positional arguments. Every sub-parser must
    # specify this one individually and here we define the kwargs to reduce
    # duplicate code.
    kinds_kwargs = {
        "dest": "kinds",
        "type": _validate_kind,
        "nargs": '+',
        "metavar": "resource",
    }

    # Sub-command GET.
    parser_get = subparsers.add_parser(
        'get', help="Get manifests from K8s and save them locally", parents=[parent]
    )
    parser_get.add_argument(**kinds_kwargs)
    parser_get.add_argument(
        "--groupby", type=str, nargs="*",
        metavar="", dest="groupby",
        help="Folder hierarchy (eg '--groupby ns kind label')",
    )

    # Sub-command DIFF.
    parser_plan = subparsers.add_parser(
        'plan', help="Diff local and K8s manifests", parents=[parent]
    )
    parser_plan.add_argument(**kinds_kwargs)

    # Sub-command PATCH.
    parser_apply = subparsers.add_parser(
        'apply', help="Patch K8s to match local manifests", parents=[parent]
    )
    parser_apply.add_argument(**kinds_kwargs)

    # Sub-command VERSION.
    subparsers.add_parser(
        'version', help="Show Square version and exit", parents=[parent]
    )

    # Parse the actual arguments.
    param = parser.parse_args()

    return param


def compile_config(cmdline_param) -> Tuple[Optional[Configuration], bool]:
    """Return `Configuration` from `cmdline_param`.

    Inputs:
        cmdline_param: SimpleNamespace

    Returns:
        Configuration, err

    """
    # Convenience.
    p = cmdline_param

    # Abort without credentials.
    if not p.kubeconfig:
        logit.error("ERROR: must either specify --kubeconfig or set KUBECONFIG")
        return None, True

    # Remove duplicates but retain the original order of "p.kinds". This is
    # a "trick" that will only work in Python 3.7+ which guarantees a stable
    # insertion order for dictionaries (but not sets).
    p.kinds = list(dict.fromkeys(p.kinds))

    # Expand the "all" resource (if present) and ignore all other resources, if
    # any were specified.
    if "all" in p.kinds:
        p.kinds = list(SUPPORTED_KINDS)

    # Folder must be `Path` object.
    folder = pathlib.Path(p.folder)

    # Specify the selectors (see definition of `dtypes.Selectors`).
    selectors = Selectors(p.kinds, p.namespaces, set(p.labels))

    # ------------------------------------------------------------------------
    # Unpack the folder hierarchy. For example:
    # From: `--groupby ns kind label=app` ->
    # To  : ManifestHierarchy(order=["ns", "kind", "label"], label="app")
    # ------------------------------------------------------------------------
    # Unpack the ordering and replace all `label=*` with `label`.
    order = getattr(p, "groupby", None) or []
    clean_order = [_ if not _.startswith("label") else "label" for _ in order]
    if not set(clean_order).issubset({"ns", "kind", "label"}):
        logit.error(f"Invalid resource names in <{order}>")
        return None, True

    labels = [_ for _ in order if _.startswith("label")]
    if len(labels) > 1:
        logit.error("Can only specify one label in file hierarchy")
        return None, True

    # Unpack the label name if the user specified it as part of `--groupby`.
    # Example, if user specified `--groupby label=app` then we need to extract
    # the `app` part. This also includes basic sanity checks.
    label_name = ""
    if len(labels) == 1:
        try:
            _, label_name = labels[0].split("=")
            assert len(label_name) > 0
        except (ValueError, AssertionError):
            logit.error(f"Invalid label specification <{labels[0]}>")
            return None, True
    groupby = ManifestHierarchy(order=clean_order, label=label_name)
    del order, clean_order, label_name

    # -------------------------------------------------------------------------
    # Assemble the full configuration and return it.
    # -------------------------------------------------------------------------
    cfg = Configuration(
        p.parser, p.verbosity, folder,
        p.kinds, p.namespaces,
        p.kubeconfig, p.ctx,
        selectors, groupby
    )
    return cfg, False


def main() -> int:
    param = parse_commandline_args()
    if param.parser == "version":
        print(__version__)
        return 0

    # Initialise logging.
    square.square.setup_logging(param.verbosity)

    # Create Square configuration from command line arguments.
    cfg, err = compile_config(param)
    if cfg is None or err:
        return 1

    # Create properly configured Requests session to talk to K8s API.
    (config, client), err = square.square.cluster_config(cfg.kubeconfig, cfg.kube_ctx)
    if err or config is None or client is None:
        return 1

    # Do what user asked us to do.
    common_args = config, client, cfg.folder, cfg.selectors
    if cfg.command == "get":
        _, err = square.square.main_get(*common_args, cfg.groupby)
    elif cfg.command == "plan":
        _, err = square.square.main_plan(*common_args)
    elif cfg.command == "apply":
        _, err = square.square.main_apply(*common_args, config.name)
    else:
        logit.error(f"Unknown command <{cfg.command}>")
        return 1

    # Return error code.
    return 1 if err else 0


if __name__ == '__main__':
    sys.exit(main())
