import argparse
import json
import logging
import os
import sys
import textwrap
from pprint import pprint

import colorama
import jsonpatch
import k8s
import manio
import yaml
from dtypes import (
    RESOURCE_ALIASES, DeltaCreate, DeltaDelete, DeltaPatch, DeploymentPlan,
    JsonPatch, RetVal,
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
      {name} diff
      {name} patch
    ''')

    def _validate_kind(kind):
        """Convert resource `kind` from aliases to canonical name.
        For instance, `svc` -> `Service`.
        """
        kind = kind.lower()
        out = [
            canonical for canonical, aliases in RESOURCE_ALIASES.items()
            if kind in aliases
        ]
        if len(out) != 1:
            raise argparse.ArgumentTypeError(kind)
        return out[0]

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
             " -v: WARNING -vv: INFO -vvv: DEBUG."
    )
    parent.add_argument(
        "-n", type=str, nargs="*",
        metavar="ns", dest="namespaces",
        help="List of namespaces (omit to operate in all namespaces)",
    )
    parent.add_argument(
        "--folder", type=str, default="./manifests/",
        help="Manifest folder (defaults='./manifests')"
    )
    parent.add_argument(
        "--kubeconfig", type=str, metavar="path", default="~/.kube/config",
        help="Location of kubeconfig file (default='~/kube/config').",
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

    # Sub-command DIFF.
    parser_diff = subparsers.add_parser(
        'diff', help="Diff local and K8s manifests", parents=[parent]
    )
    parser_diff.add_argument(**kinds_kwargs)

    # Sub-command PATCH.
    parser_patch = subparsers.add_parser(
        'patch', help="Patch K8s to match local manifests", parents=[parent]
    )
    parser_patch.add_argument(**kinds_kwargs)

    # Parse the actual arguments.
    return parser.parse_args()


def make_patch(config, local: dict, server: dict):
    """Return JSON patch to transition `server` to `local`.

    Inputs:
        local_manifests: dict
            Usually the dictionary keys returned by `load_manifest`.
        server_manifests: dict
            Usually the dictionary keys returned by `manio.download`.

    Returns:
        Patch: the JSON patch and human readable diff in a `Patch` tuple.

    """
    # Reduce local and server manifests to salient fields (ie apiVersion, kind,
    # metadata and spec). Abort on error.
    local, err1 = manio.strip(config, local)
    server, err2 = manio.strip(config, server)
    if err1 or err2:
        return RetVal(None, True)

    # Sanity checks: abort if the manifests do not specify the same resource.
    try:
        assert server.apiVersion == local.apiVersion
        assert server.kind == local.kind
        assert server.metadata.name == local.metadata.name
        if server.kind != "Namespace":
            assert server.metadata.namespace == local.metadata.namespace
    except AssertionError:
        # Log the invalid manifests and return with an error.
        keys = ("apiVersion", "kind", "metadata")
        local = {k: local[k] for k in keys}
        server = {k: server[k] for k in keys}
        logit.error(
            "Cannot compute JSON patch for incompatible manifests. "
            f"Local: <{local}>  Server: <{server}>"
        )
        return RetVal(None, True)

    # Determine the PATCH URL for the resource.
    namespace = server.metadata.namespace if server.kind != "Namespace" else None
    url, err = k8s.urlpath(config, server.kind, namespace)
    if err:
        return RetVal(None, True)
    full_url = f'{url}/{server.metadata.name}'

    # Compute JSON patch.
    patch = jsonpatch.make_patch(server, local)
    patch = json.loads(patch.to_string())

    # Return the patch.
    return RetVal(JsonPatch(full_url, patch), False)


def partition_manifests(local_manifests, server_manifests):
    """Compile `{local,server}_manifests` into CREATE, PATCH and DELETE groups.

    The returned deployment plan will contain *every* resource in
    `local_manifests` and `server_manifests` *exactly once*. Their relative
    order will also be preserved.

    Create: all resources that exist in `local_manifests` but not in `server_manifests`.
    Delete: all resources that exist in `server_manifests` but not in `local_manifests`.
    Patch : all resources that exist in both and therefore *may* need patching.

    Inputs:
        local_manifests: Dict[MetaManifest:str]
            Usually the dictionary keys returned by `load_manifest`.
        server_manifests: Dict[MetaManifest:str]
            Usually the dictionary keys returned by `manio.download`.

    Returns:
        DeploymentPlan

    """
    # Determine what needs adding, removing and patching to steer the K8s setup
    # towards what `local_manifests` specifies.
    loc = set(local_manifests.keys())
    srv = set(server_manifests.keys())
    create = loc - srv
    patch = loc.intersection(srv)
    delete = srv - loc

    # Convert the sets to list. Preserve the relative element ordering as it
    # was in `{local_server}_manifests`.
    create = [_ for _ in local_manifests if _ in create]
    patch = [_ for _ in local_manifests if _ in patch]
    delete = [_ for _ in server_manifests if _ in delete]

    # Return the deployment plan.
    plan = DeploymentPlan(create, patch, delete)
    return RetVal(plan, False)


def compile_plan(config, local_manifests, server_manifests):
    """Return the `DeploymentPlan` to transition K8s to state of `local_manifests`.

    The deployment plan is a named tuple. It specifies which resources to
    create, patch and delete to ensure that the state of K8s matches that
    specified in `local_manifests`.

    Inputs:
        config: k8s.Config
        local_manifests: Dict[MetaManifest, dict]
            Should be output from `load_manifest` or `load`.
        server_manifests: Dict[MetaManifest, dict]
            Should be output from `manio.download`.

    Returns:
        DeploymentPlan

    """
    # Partition the set of meta manifests into create/delete/patch groups.
    plan, err = partition_manifests(local_manifests, server_manifests)
    if err:
        return RetVal(None, True)

    # Sanity check: the resources to patch *must* exist in both local and
    # server manifests. This is a bug if not.
    assert set(plan.patch).issubset(set(local_manifests.keys()))
    assert set(plan.patch).issubset(set(server_manifests.keys()))

    # Compile the Deltas to create the missing resources.
    create = []
    for delta in plan.create:
        url, err = k8s.urlpath(config, delta.kind, namespace=delta.namespace)
        if err:
            return RetVal(None, True)
        create.append(DeltaCreate(delta, url, local_manifests[delta]))

    # Compile the Deltas to delete the excess resources. Every DELETE request
    # will have to pass along a `DeleteOptions` manifest (see below).
    del_opts = {
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "gracePeriodSeconds": 0,
        "orphanDependents": False,
    }
    delete = []
    for meta in plan.delete:
        # Resource URL.
        url, err = k8s.urlpath(config, meta.kind, namespace=meta.namespace)
        if err:
            return RetVal(None, True)

        # DELETE requests must specify the resource name in the path.
        url = f"{url}/{meta.name}"

        # Assemble the meta and add it to the list.
        delete.append(DeltaDelete(meta, url, del_opts.copy()))

    # Iterate over each manifest that needs patching and determine the
    # necessary JSON Patch to transition K8s into the state specified in the
    # local manifests.
    patches = []
    for meta in plan.patch:
        loc = local_manifests[meta]
        srv = server_manifests[meta]

        # Compute textual diff (only useful for the user to study the diff).
        diff_str, err = manio.diff(config, loc, srv)
        if err:
            return RetVal(None, True)

        # Compute the JSON patch that will match K8s to the local manifest.
        patch, err = make_patch(config, loc, srv)
        if err:
            return RetVal(None, True)
        patches.append(DeltaPatch(meta, diff_str, patch))

    # Assemble and return the deployment plan.
    return RetVal(DeploymentPlan(create, patches, delete), False)


def print_deltas(plan):
    """Print human readable version of `plan` to terminal.

    Inputs:
        plan: DeploymentPlan

    Returns:
        None

    """
    # Terminal colours for convenience.
    cGreen = colorama.Fore.GREEN
    cRed = colorama.Fore.RED
    cReset = colorama.Fore.RESET

    # Use Green to list all the resources that we should create.
    for delta in plan.create:
        name = f'{delta.meta.namespace}/{delta.meta.name}'

        # Convert manifest to YAML string and print every line in Green.
        txt = yaml.dump(delta.manifest, default_flow_style=False)
        txt = [cGreen + line + cReset for line in txt.splitlines()]

        # Add header line.
        txt.insert(0, cGreen + f"--- {name} ---" + cReset)

        # Print the reassembled string.
        print(str.join('\n', txt) + '\n')

    # Print the diff (already contains terminal colours) for all the resources
    # that we should patch.
    deltas = plan.patch
    for delta in deltas:
        if len(delta.diff) == 0:
            continue

        # Add some terminal colours to make it look prettier.
        formatted_diff = []
        for line in delta.diff.splitlines():
            if line.startswith('+'):
                formatted_diff.append(colorama.Fore.GREEN + line + colorama.Fore.RESET)
            elif line.startswith('-'):
                formatted_diff.append(colorama.Fore.RED + line + colorama.Fore.RESET)
            else:
                formatted_diff.append(line)
        formatted_diff = str.join('\n', formatted_diff)

        name = f'{delta.meta.namespace}/{delta.meta.name}'
        print('-' * 80 + '\n' + f'{name.upper()}\n' + '-' * 80)
        print(formatted_diff)

    # Use Red to list all the resources that we should delete.
    for delta in plan.delete:
        name = f'--- {delta.meta.namespace}/{delta.meta.name} ---'
        print(cRed + name + cReset + "\n")

    return RetVal(None, False)


def find_namespace_orphans(meta_manifests):
    """Return all orphaned resources in the `meta_manifest` set.

    A resource is orphaned iff it lives in a namespace that is not explicitly
    declared in the set of `meta_manifests`.

    This function is particularly useful to verify a set of local manifests and
    pinpoint resources that someone forgot to delete and that have a typo in
    their namespace field.

    Inputs:
        meta_manifests: Iterable[MetaManifest]

    Returns:
        set[MetaManifest]: orphaned resources.

    """
    # Turn the input into a set.
    meta_manifests = set(meta_manifests)

    # Extract all declared namespaces so we can find the orphans afterwards.
    namespaces = {_.name for _ in meta_manifests if _.kind == 'Namespace'}

    # Filter all those manifests that are a) not a Namespace and b) live in a
    # namespace that does not exist in the `namespaces` set aggregated earlier.
    orphans = {
        _ for _ in meta_manifests
        if _.kind != "Namespace" and _.namespace not in namespaces
    }

    # Return the result.
    return RetVal(orphans, None)


def setup_logging(level: int):
    """Configure logging at `level`.

    Level 0: ERROR
    Level 1: WARNING
    Level 2: INFO
    Level >=3: DEBUG

    Inputs:
        level: int

    Returns:
        None

    """
    # Pick the correct log level.
    if level == 0:
        level = "ERROR"
    elif level == 1:
        level = "WARNING"
    elif level == 2:
        level = "INFO"
    else:
        level = "DEBUG"

    # Create logger.
    logger = logging.getLogger("square")
    logger.setLevel(level)

    # Configure stdout handler.
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    # Attach stdout handlers to the `square` logger.
    logger.addHandler(ch)


def prune(manifests, kinds, namespaces):
    """Return the `manifests` subset that meets the requirement.

    This is really just a glorified list comprehension without error
    checking of any kind.

    Inputs:
        manifests: Dict[MetaManifest:dict]
        kinds: Iterable
            Allowed resource types (eg ["Deployment", "Namespace"]).
        namespaces: Iterable
            Only use those namespaces. Set to `None` to use all.

    Returns:
        Dict[MetaManifest:dict]: subset of input `manifests`.

    """
    # Compile the list of manifests that have the correct resource kind.
    out = {k: v for k, v in manifests.items() if k.kind in kinds}

    # Remove all resources outside the specified namespaces. Retain all
    # resources if `namespaces` is None.
    if namespaces is not None:
        out = {k: v for k, v in out.items() if k.namespace in namespaces}
    return out


def main_patch(config, client, folder, kinds, namespaces):
    """Update K8s to match the specifications in `local_manifests`.

    Create a deployment plan that will transition the K8s state
    `server_manifests` to the desired `local_manifests`.

    Inputs:
        config: k8s.Config
        client: `requests` session with correct K8s certificates.
        folder: Path
            Path to local manifests eg "./foo"
        kinds: Iterable
            Resource types to fetch, eg ["Deployment", "Namespace"]
        namespaces: Iterable
            Only use those namespaces. Set to `None` to use all.

    Returns:
        None

    """
    try:
        # Load manifests from local files.
        local, err = manio.load(folder)
        assert not err

        # Download manifests from K8s.
        server, err = manio.download(config, client, kinds, namespaces)
        assert not err

        # Prune the manifests to only include manifests for the specified
        # resources and namespaces. The pruning is not technically necessary
        # for the `server` manifests but does not hurt.
        local = prune(local.meta, kinds, namespaces)
        server = prune(server, kinds, namespaces)

        # Create the deployment plan.
        plan, err = compile_plan(config, local, server)
        assert not err

        # Present the plan confirm to go ahead with the user.
        print_deltas(plan)

        # Create the missing resources.
        for data in plan.create:
            print(f"Creating {data.meta.namespace}/{data.meta.name}")
            _, err = k8s.post(client, data.url, data.manifest)
            assert not err

        # Patch the server resources.
        patches = [_.patch for _ in plan.patch if len(_.patch.ops) > 0]
        print(f"Compiled {len(patches)} patches.")
        for patch in patches:
            pprint(patch)
            _, err = k8s.patch(client, patch.url, patch.ops)
            assert not err

        # Delete the excess resources.
        for data in plan.delete:
            print(f"Deleting {data.meta.namespace}/{data.meta.name}")
            _, err = k8s.delete(client, data.url, data.manifest)
            assert not err
    except AssertionError:
        return RetVal(None, True)

    # All good.
    return RetVal(None, False)


def main_diff(config, client, folder, kinds, namespaces):
    """Print the diff between `local_manifests` and `server_manifests`.

    The diff shows what would have to change on the K8s server in order for it
    to match the setup defined in `local_manifests`.

    Inputs:
        config: k8s.Config
        client: `requests` session with correct K8s certificates.
        folder: Path
            Path to local manifests eg "./foo"
        kinds: Iterable
            Resource types to fetch, eg ["Deployment", "Namespace"]
        namespaces: Iterable
            Only use those namespaces. Set to `None` to use all.

    Returns:
        None

    """
    try:
        # Load manifests from local files.
        local, err = manio.load(folder)
        assert not err

        # Download manifests from K8s.
        server, err = manio.download(config, client, kinds, namespaces)
        assert not err

        # Prune the manifests to only include manifests for the specified
        # resources and namespaces. The pruning is not technically necessary
        # for the `server` manifests but does not hurt.
        local = prune(local.meta, kinds, namespaces)
        server = prune(server, kinds, namespaces)

        # Create deployment plan.
        plan, err = compile_plan(config, local, server)
        assert not err
    except AssertionError:
        return RetVal(None, True)

    # Print the plan and return.
    print_deltas(plan)
    return RetVal(None, False)


def main_get(config, client, folder, kinds, namespaces):
    """Download all K8s manifests and merge them into local files.

    Inputs:
        config: k8s.Config
        client: `requests` session with correct K8s certificates.
        folder: Path
            Path to local manifests eg "./foo"
        kinds: Iterable
            Resource types to fetch, eg ["Deployment", "Namespace"]
        namespaces: Iterable
            Only use those namespaces. Set to `None` to use all.

    Returns:
        None

    """
    try:
        # Load manifests from local files.
        local, err = manio.load(folder)
        assert not err

        # Download manifests from K8s.
        server, err = manio.download(config, client, kinds, namespaces)
        assert not err

        # Prune the manifests to only include manifests for the specified
        # resources and namespaces. This is not technically necessary
        # for the `server` manifests but does not hurt and makes it consistent
        # with how the other main_* functions operate.
        server = prune(server, kinds, namespaces)

        # Sync the server manifests into the local manifests. All this happens in
        # memory and no files will be modified here - see next step.
        synced_manifests, err = manio.sync(local.files, server, kinds, namespaces)
        assert not err

        # Write the new manifest files.
        _, err = manio.save(folder, synced_manifests)
        assert not err
    except AssertionError:
        return RetVal(None, True)

    # Success.
    return RetVal(None, False)


def main():
    param = parse_commandline_args()

    # Initialise logging.
    setup_logging(param.verbosity)

    # Create a `requests` client with proper security certificates to access
    # K8s API.
    kubeconfig = os.path.expanduser(param.kubeconfig)
    config = k8s.load_auto_config(kubeconfig, disable_warnings=True)
    client = k8s.session(config)

    # Update the config with the correct K8s API version.
    config, err = k8s.version(config, client)
    if err:
        sys.exit(1)

    # Do what user asked us to do.
    if param.parser == "get":
        _, err = main_get(config, client, param.folder, param.kinds, param.namespaces)
    elif param.parser == "diff":
        _, err = main_diff(config, client, param.folder, param.kinds, param.namespaces)
    elif param.parser == "patch":
        _, err = main_patch(config, client, param.folder, param.kinds, param.namespaces)
    else:
        logit.error(f"Unknown command <{param.parser}>")
        err = True

    # Abort with non-zero exit code if there was an error.
    if err:
        sys.exit(1)


if __name__ == '__main__':
    main()
