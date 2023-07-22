import argparse
import logging
import os
from typing import Any, Dict, Optional, Tuple

import colorama

import square
import square.k8s
import square.square
from square import DEFAULT_CONFIG_FILE, __version__
from square.dtypes import Config, Filepath, GroupBy, Selectors

# Convenience: global logger instance to avoid repetitive code.
logit = logging.getLogger("square")


def parse_commandline_args():
    """Return parsed command line."""
    def _validate_label(label: str) -> str:
        """Sanity check the label"""
        if not square.square.valid_label(label):
            raise argparse.ArgumentTypeError(label)
        return label

    # A dummy top level parser that will become the parent for all sub-parsers
    # to share all its arguments.
    parent = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
        prog="square",
    )
    parent.add_argument(
        "-v", "--verbosity", action="count", default=0,
        help="Log level (-v: WARNING -vv: INFO -vvv: DEBUG)"
    )
    parent.add_argument(
        "--info", action='store_true',
        help="Print compiled configuration (useful to debug)"
    )
    parent.add_argument(
        "-c", "--config", type=str, default="", dest="configfile",
        help="Read configuration from this file"
    )
    parent.add_argument(
        "--no-config", dest="no_config", action="store_true",
        help="Ignore .square.yaml file"
    )
    parent.add_argument(
        "-n", "--namespace", "--namespaces", type=str, nargs="*",
        metavar="ns", dest="namespaces", default=None,
        help="List of namespaces (omit to select all)",
    )
    parent.add_argument(
        "-l", "--label", "--labels", type=_validate_label, nargs="*",
        metavar="labels", dest="labels", default=None,
        help="Target only these labels (eg 'app=foo')",
    )
    parent.add_argument(
        "-p", "--priorities", nargs="*",
        metavar="priorities", dest="priorities", default=None,
        help="Sort resource in this order when saving and applying",
    )
    parent.add_argument(
        "--kubeconfig", type=str, metavar="path",
        default=None, help="Location of kubeconfig file",
    )
    parent.add_argument(
        "--kubecontext", type=str, metavar="kubecontext", default=None,
        help="Kubernetes context (defaults to default context)",
    )
    parent.add_argument(
        "--folder", type=str, metavar="path", default=None,
        help="Manifest folder (defaults to ./)",
    )

    # The primary parser for the top level options (eg GET, PATCH, ...).
    parser = argparse.ArgumentParser(add_help=True, prog="square")
    subparsers = parser.add_subparsers(
        help='Mode', dest='parser', metavar="ACTION",
        title="Operation", required=True,
    )

    # Configuration for `kinds` positional arguments. Every sub-parser must
    # specify this one individually and here we define the kwargs to reduce
    # duplicated code.
    kinds_kwargs: Dict[str, Any] = {
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

    # Set `param.kinds` to `None` if the user did not specify any. This works
    # around a argparse idiosyncrasy where one cannot specify a `None` default
    # value for an optional list of positional arguments.
    kinds = getattr(param, "kinds", None)
    param.kinds = kinds if kinds else None

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
        selectors=Selectors(),
        groupby=GroupBy(label="", order=[]),
        priorities=[],
    ), True

    # Convenience.
    p = cmdline_param

    # Load the default configuration unless the user specified an explicit one.
    if p.configfile:
        logit.info(f"Loading configuration file <{p.configfile}>")
        cfg, err = square.cfgfile.load(p.configfile)

        # Look for `--kubeconfig`. Defaults to the value in config file.
        kubeconfig = p.kubeconfig or cfg.kubeconfig
    else:
        # Pick the configuration file. Depends on whether the user specified
        # `--no-config`, `--config` and if `.square.yaml` exists.
        default_cfg = DEFAULT_CONFIG_FILE
        dot_square = Filepath(".square.yaml")

        if p.no_config:
            cfg_file = default_cfg
        else:
            cfg_file = dot_square if dot_square.exists() else default_cfg

        logit.info(f"Loading configuration file <{cfg_file}>")
        cfg, err = square.cfgfile.load(cfg_file)

        # Determine which Kubeconfig to use. The order is: `--kubeconfig`,
        # `--config`, `.square`, `KUBECONFIG` environment variable.
        if cfg_file == default_cfg:
            kubeconfig = p.kubeconfig or os.getenv("KUBECONFIG", "")
            cfg.folder = Filepath.cwd()
        else:
            kubeconfig = p.kubeconfig or str(cfg.kubeconfig) or os.getenv("KUBECONFIG", "")  # noqa

        del dot_square, default_cfg

        # Abort if neither `--kubeconfig` nor the KUBECONFIG environment variable exist.
        if not kubeconfig:
            logit.error("Must specify a Kubernetes config file.")
            return err_resp
    if err:
        return err_resp

    # Override the folder if user specified "--folder".
    folder = cfg.folder if p.folder is None else p.folder

    # Expand the user's home folder, ie if the path contains a "~".
    folder = Filepath(folder).expanduser()
    kubeconfig = Filepath(kubeconfig).expanduser()

    # ------------------------------------------------------------------------
    # GroupBy (determines the folder hierarchy that GET will create).
    # In : `--groupby ns kind label=app`
    # Out: GroupBy(order=["ns", "kind", "label"], label="app")
    # ------------------------------------------------------------------------
    # Unpack the ordering and replace all `label=*` with `label`.
    # NOTE: `p.groups` does not necessarily exist because the option only makes
    #        sense for eg `square GET` and is thus not implemented for `square apply`.
    order = getattr(p, "groupby", None)
    if order is None:
        groupby = cfg.groupby
    else:
        clean_order = [_ if not _.startswith("label") else "label" for _ in order]
        if not set(clean_order).issubset({"ns", "kind", "label"}):
            logit.error("Invalid definition of `groupby`")
            return err_resp

        labels = [_ for _ in order if _.startswith("label")]
        if len(labels) > 1:
            logit.error("Can only specify one `label=<name>` in `--groupby`.")
            return err_resp

        # Unpack the label name from the `--groupby` argument.
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

        # Compile the final `GroupBy` structure.
        groupby = GroupBy(order=list(clean_order), label=label_name)
        del order, clean_order, label_name

    # ------------------------------------------------------------------------
    # General: folder, kubeconfig, kubecontext, ...
    # ------------------------------------------------------------------------
    kinds = set(p.kinds) if p.kinds else cfg.selectors.kinds

    # Use the value from the (default) config file unless the user overrode
    # them on the command line.
    kubeconfig = Filepath(kubeconfig)
    kubecontext = p.kubecontext or cfg.kubecontext
    namespaces = cfg.selectors.namespaces if p.namespaces is None else p.namespaces
    sel_labels = cfg.selectors.labels if p.labels is None else p.labels
    priorities = p.priorities or cfg.priorities
    selectors = Selectors(kinds=kinds, namespaces=namespaces, labels=sel_labels)

    # Use filters from (default) config file because they cannot be specified
    # on the command line.
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
        patch_callback=None,
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


def expand_all_kinds(cfg: Config) -> Tuple[Config, bool]:
    """Replace an empty `cfg.selector.kinds` with all available resources.

    Does nothing if `cfg.selectors.kinds` is non-empty.

    """
    # Do nothing if the user specified at least one resource type.
    if len(cfg.selectors.kinds) > 0:
        return cfg, False

    # Create a K8sConfig instance because it will contain all the info we need.
    k8sconfig, err = square.k8s.cluster_config(cfg.kubeconfig, cfg.kubecontext)
    if err:
        return (cfg, True)

    # Replace the empty resource list with all KINDS that K8s has to offer.
    cfg.selectors.kinds.clear()
    cfg.selectors.kinds.update(k8sconfig.kinds)
    return (cfg, False)


def show_info(cfg: Config) -> bool:
    """Show some info about the configuration that will be used."""
    sel = cfg.selectors

    print()
    print(f"Kubeconfig : {cfg.kubeconfig} (Context={cfg.kubecontext})")
    print("Manifests  :", cfg.folder)
    print("Namespaces :", sel.namespaces if sel.namespaces else "all")
    print("Kinds      :", sel.kinds)
    print("Labels     :", sel.labels)
    return False


def main() -> int:
    param = parse_commandline_args()

    # Print version information and quit.
    if param.parser == "version":
        print(__version__)
        return 0

    # Create a default ".square.yaml" in the current folder and quit.
    if param.parser == "config":
        fname = Filepath(param.folder or ".") / ".square.yaml"
        fname.parent.mkdir(parents=True, exist_ok=True)
        fname.write_text(DEFAULT_CONFIG_FILE.read_text())
        print(
            f"Created configuration file <{fname}>.\n"
            "Please open the file in an editor and adjust the values, most notably "
            "`kubeconfig` and `selectors.[kinds | namespaces | labels]`."
        )
        return 0

    # Initialise logging.
    square.square.setup_logging(param.verbosity)

    # Create Square configuration from command line arguments.
    cfg, err1 = compile_config(param)
    cfg, err2 = expand_all_kinds(cfg)
    if err1 or err2:
        return 1

    # Do what the user asked us to do.
    if param.info:
        err = show_info(cfg)
    elif param.parser == "get":
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
