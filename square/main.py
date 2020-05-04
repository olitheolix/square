import argparse
import logging
import os
import re
import sys
import textwrap
from typing import Optional, Tuple

import colorama
import square
import square.square
from square import __version__
from square.dtypes import Config, Filepath, GroupBy, Selectors

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
        help="Log level (-v: WARNING -vv: INFO -vvv: DEBUG)"
    )
    parent.add_argument(
        "-c", "--config", type=str, default="",
        help="Read configuration from this file"
    )
    parent.add_argument(
        "-n", "--namespace", type=str, nargs="*",
        metavar="ns", dest="namespaces", default=None,
        help="List of namespaces (omit to consider all)",
    )
    parent.add_argument(
        "-l", "--labels", type=_validate_label, nargs="*",
        metavar="labels", dest="labels", default=None,
        help="Only select resources with these labels (eg 'app=foo')",
    )
    parent.add_argument(
        "-p", "--priorities", nargs="*",
        metavar="priorities", dest="priorities", default=None,
        help="Sort resource in this order when saving and applying",
    )
    parent.add_argument(
        "--kubeconfig", type=str, metavar="path",
        default=os.environ.get("KUBECONFIG", None),
        help="Location of kubeconfig file (defaults to env KUBECONFIG)",
    )
    parent.add_argument(
        "--folder", type=str, metavar="path", default="./",
        help="Manifest folder (defaults to ./)",
    )
    parent.add_argument(
        "--context", type=str, metavar="ctx", dest="ctx", default=None,
        help="Kubernetes context (defaults to default context)",
    )

    # The primary parser for the top level options (eg GET, PATCH, ...).
    parser = argparse.ArgumentParser(add_help=True)
    subparsers = parser.add_subparsers(
        help='Mode', dest='parser', metavar="ACTION",
        title="Operation", required=True
    )

    # Configuration for `kinds` positional arguments. Every sub-parser must
    # specify this one individually and here we define the kwargs to reduce
    # duplicated code.
    kinds_kwargs = {
        "dest": "kinds",
        "type": str,
        "nargs": '*',
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
        help="Folder hierarchy (eg '--groupby ns label=app kind')",
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

    # Sub-command CONFIG.
    subparsers.add_parser(
        'config', help="Create .square.yaml (works with --folder)", parents=[parent]
    )

    # Parse the actual arguments.
    param = parser.parse_args()

    return param


def user_confirmed(answer: Optional[str] = "yes") -> bool:
    """Return True iff the user answers with `answer` or `answer` is None."""
    if answer is None:
        return True

    assert answer, "BUG: desired answer must be non-empty string or `None`"

    cDel = colorama.Fore.RED
    cReset = colorama.Fore.RESET

    # Confirm with user.
    print(f"Type {cDel}{answer}{cReset} to apply the plan.")
    try:
        return input("  Your answer: ") == answer
    except KeyboardInterrupt:
        print()
        return False


def compile_config(cmdline_param) -> Tuple[Config, bool]:
    """Return `Config` from `cmdline_param`.

    Inputs:
        cmdline_param: SimpleNamespace

    Returns:
        Config, err

    """
    err_resp = Config(
        folder=Filepath(""),
        kubeconfig=Filepath(""),
        kubecontext=None,
        selectors=Selectors(set(), namespaces=[], labels=set()),
        groupby=GroupBy("", []),
        priorities=[],
    ), True

    # Convenience.
    p = cmdline_param

    sample_fname = Filepath("resources/sampleconfig.yaml")

    # Exclusively use the config file if the user specified one.
    cfg_file = p.config or sample_fname
    cfg, err = square.square.load_config(cfg_file)
    if err:
        return err_resp

    # ------------------------------------------------------------------------
    # GroupBy (determines the folder hierarchy that GET will create).
    # In : `--groupby ns kind label=app`
    # Out: GroupBy(order=["ns", "kind", "label"], label="app")
    # ------------------------------------------------------------------------
    # Unpack the ordering and replace all `label=*` with `label`.
    if getattr(p, "groupby", None) is None:
        groupby = cfg.groupby
    else:
        order = getattr(p, "groupby", None) or []
        clean_order = [_ if not _.startswith("label") else "label" for _ in order]
        if not set(clean_order).issubset({"ns", "kind", "label"}):
            logit.error(f"Invalid resource names in <{order}>")
            return err_resp

        labels = [_ for _ in order if _.startswith("label")]
        if len(labels) > 1:
            logit.error("Can only specify one label in file hierarchy")
            return err_resp

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
                return err_resp
        groupby = GroupBy(order=list(clean_order), label=label_name)
        del order, clean_order, label_name

    # ------------------------------------------------------------------------
    # General: folder, kubeconfig, kubecontext, ...
    # ------------------------------------------------------------------------
    # Remove duplicates but retain the original order of "p.kinds". This is
    # a "trick" that will only work in Python 3.7+ which guarantees a stable
    # insertion order for dictionaries (but not sets).
    kinds = cfg.selectors.kinds if p.kinds is None else set(dict.fromkeys(p.kinds))
    namespaces = cfg.selectors.namespaces if p.namespaces is None else p.namespaces
    labels = cfg.selectors.labels if p.labels is None else set(p.labels)

    folder = Filepath(p.folder or cfg.folder)
    kubeconfig = Filepath(p.kubeconfig) or Filepath(cfg.kubeconfig)
    kubecontext = p.ctx or cfg.kubecontext
    priorities = p.priorities or cfg.priorities
    selectors = Selectors(kinds, namespaces, labels)
    filters = cfg.filters

    # ------------------------------------------------------------------------
    # Verify inputs.
    # ------------------------------------------------------------------------
    # Abort without credentials.
    if not kubeconfig.exists():
        logit.error(f"Cannot find Kubernetes config file <{kubeconfig}>")
        return err_resp

    # -------------------------------------------------------------------------
    # Assemble the full configuration and return it.
    # -------------------------------------------------------------------------
    cfg = Config(
        folder=folder,
        kubeconfig=kubeconfig,
        kubecontext=kubecontext,
        selectors=selectors,
        groupby=groupby,
        priorities=priorities,
        filters=filters,
    )
    return cfg, False


def apply_plan(cfg: Config, confirm_string: Optional[str]) -> bool:
    """Update K8s to match the specifications in `local_manifests`.

    Create a deployment plan that will transition the K8s state
    `server_manifests` to the desired `local_manifests`.

    Inputs:
        cfg: Square configuration.
        confirm_string:
            Only apply the plan if user answers with this string in the
            confirmation dialog (set to `None` to disable confirmation).

    """
    try:
        # Obtain the plan.
        plan, err = square.square.make_plan(cfg)
        assert not err and plan

        # Exit prematurely if there are no changes to apply.
        num_patch_ops = sum([len(_.patch.ops) for _ in plan.patch])
        if len(plan.create) == len(plan.delete) == num_patch_ops == 0:
            print("Nothing to change")
            return False
        del num_patch_ops

        # Print the plan and ask for user confirmation. Abort if the user does
        # not give it.
        square.square.show_plan(plan)
        if not user_confirmed(confirm_string):
            print("User abort - no changes were made.")
            return True
        print()

        # Apply the plan.
        assert not square.square.apply_plan(cfg, plan)
    except AssertionError:
        return True

    # All good.
    return False


def sanitise_resource_kinds(cfg: Config) -> Tuple[Config, bool]:
    """Populate the `Selector.kinds` with those the user specified on the commandline."""
    # Create a K8sConfig instance because it will contain all the info we need.
    k8sconfig, err = square.k8s.cluster_config(cfg.kubeconfig, cfg.kubecontext)
    if err:
        return (cfg, True)

    # If the user did not specify any selectors then we assume he wants all resources.
    if len(cfg.selectors.kinds) == 0:
        cfg.selectors.kinds.update(k8sconfig.kinds)

    # Translate colloquial names to their canonical counterpart if possible.
    # Example: "svc" -> "Service"
    # Do nothing if we do not recognise the name. This can happen because the
    # user made a typo, or the resource is a custom resource that is not yet
    # known. Since we cannot distinguish them, we allow typos and then ignore
    # the resource during the get/plan/apply cycle.
    kinds = {k8sconfig.short2kind.get(_.lower(), _) for _ in cfg.selectors.kinds}

    # Replace the (possibly) colloquial names with their correct K8s kinds.
    cfg.selectors.kinds.clear()
    cfg.selectors.kinds.update(kinds)
    return (cfg, False)


def main() -> int:
    param = parse_commandline_args()
    if param.parser == "version":
        print(__version__)
        return 0

    if param.parser == "config":
        fname = Filepath(param.folder) / ".square.yaml"
        fname.parent.mkdir(parents=True, exist_ok=True)
        fname.write_text(open("resources/sampleconfig.yaml").read())
        print(
            f"Created configuration file <{fname}>.\n"
            "Please open the file in an editor and adjust the values, most notably "
            "<kubeconfig>, <kubecontext>, <folder> and <selectors.labels>."
        )
        return 0

    # Initialise logging.
    square.square.setup_logging(param.verbosity)

    # Create Square configuration from command line arguments.
    cfg, err1 = compile_config(param)
    cfg, err2 = sanitise_resource_kinds(cfg)
    if err1 or err2:
        return 1

    # Do what the user asked us to do.
    if param.parser == "get":
        err = square.square.get_resources(cfg)
    elif param.parser == "plan":
        plan, err = square.square.make_plan(cfg)
        square.square.show_plan(plan)
    elif param.parser == "apply":
        err = apply_plan(cfg, "yes")
    else:
        logit.error(f"Unknown command <{param.parser}>")
        return 1

    # Return error code.
    return 1 if err else 0


if __name__ == '__main__':
    sys.exit(main())
