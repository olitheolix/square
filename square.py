import argparse
import json
import logging
import os
import re
import sys
import textwrap
from pprint import pprint

import colorama
import jsonpatch
import k8s
import manio
import yaml
from dtypes import (
    SUPPORTED_KINDS, SUPPORTED_VERSIONS, DeltaCreate, DeltaDelete, DeltaPatch,
    DeploymentPlan, JsonPatch, RetVal,
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

    parent = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False
    )
    parent.add_argument("-v", "--verbosity", action="count", default=0)

    parser = argparse.ArgumentParser(add_help=True)
    subparsers = parser.add_subparsers(
        help='Mode', dest='parser', metavar="ACTION",
        title="Operation", required=True
    )

    # Sub-command GET.
    parser_get = subparsers.add_parser(
        'get', help="Get manifests", parents=[parent]
    )
    padd = parser_get.add_argument
    padd('kind', type=str, nargs='*')
    del parser_get, padd

    # Sub-command DIFF.
    parser_diff = subparsers.add_parser(
        'diff', help="Diff local and server manifests", parents=[parent]
    )
    padd = parser_diff.add_argument
    padd('kind', type=str, nargs='*')
    del parser_diff, padd

    # Sub-command PATCH.
    parser_patch = subparsers.add_parser(
        'patch', help="Patch server", parents=[parent]
    )
    padd = parser_patch.add_argument
    padd('kind', type=str, nargs='*')

    # Parse the actual arguments.
    return parser.parse_args()


def urlpath(config, kind, namespace):
    """Return complete URL to K8s resource.

    Inputs:
        config: k8s.Config
        kind: str
            Eg "Deployment", "Namespace", ... (case sensitive).
        namespace: str
            Must be None for "Namespace" resources.

    Returns:
        str: Full path K8s resource, eg https://1.2.3.4/api/v1/namespace/foo/services.

    """
    # Namespaces are special because they lack the `namespaces/` path prefix.
    if kind == "Namespace" or namespace is None:
        namespace = ""
    else:
        # Namespace name must conform to K8s standards.
        match = re.match(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", namespace)
        if match is None or match.group() != namespace:
            logit.error(f"Invalid namespace name <{namespace}>.")
            return RetVal(None, True)

        namespace = f"namespaces/{namespace}/"

    # We must support the specified resource kind.
    if kind not in SUPPORTED_KINDS:
        logit.error(f"Unsupported resource <{kind}>.")
        return RetVal(None, True)

    # We must support the specified K8s version.
    if config.version not in SUPPORTED_VERSIONS:
        logit.error(f"Unsupported K8s version <{config.version}>.")
        return RetVal(None, True)

    # The HTTP request path names by K8s version and resource kind.
    # The keys in this dict must cover all those specified in
    # `SUPPORTED_VERSIONS` and `SUPPORTED_KINDS`.
    resources = {
        "1.9": {
            "ConfigMap": f"api/v1/{namespace}/configmaps",
            "Secret": f"api/v1/{namespace}/secrets",
            "Deployment": f"apis/extensions/v1beta1/{namespace}/deployments",
            "Ingress": f"apis/extensions/v1beta1/{namespace}/ingresses",
            "Namespace": f"api/v1/namespaces",
            "Service": f"api/v1/{namespace}/services",
        },
        "1.10": {
            "ConfigMap": f"api/v1/{namespace}/configmaps",
            "Secret": f"api/v1/{namespace}/secrets",
            "Deployment": f"apis/extensions/v1beta1/{namespace}/deployments",
            "Ingress": f"apis/extensions/v1beta1/{namespace}/ingresses",
            "Namespace": f"api/v1/namespaces",
            "Service": f"api/v1/{namespace}/services",
        },
    }

    # Look up the resource path and remove duplicate "/" characters (may have
    # slipped in when no namespace was supplied, eg "/api/v1//configmaps").
    path = resources[config.version][kind]
    path = path.replace("//", "/")
    assert not path.startswith("/")

    # Return the complete resource URL.
    return RetVal(f"{config.url}/{path}", False)


def make_patch(config, local: dict, server: dict):
    """Return JSON patch to transition `server` to `local`.

    Inputs:
        local_manifests: dict
            Usually the dictionary keys returned by `load_manifest`.
        server_manifests: dict
            Usually the dictionary keys returned by `download_manifests`.

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
    url, err = urlpath(config, server.kind, namespace)
    if err:
        return RetVal(None, True)
    full_url = f'{url}/{server.metadata.name}'

    # Compute JSON patch.
    patch = jsonpatch.make_patch(server, local)
    patch = json.loads(patch.to_string())

    # Return the patch.
    return RetVal(JsonPatch(full_url, patch), False)


def download_manifests(config, client, kinds, namespaces):
    """Download and return the specified resource `kinds`.

    Set `namespace` to None to download from all namespaces.

    Either returns all the data or an error, never partial results.

    Inputs:
        config: k8s.Config
        client: `requests` session with correct K8s certificates.
        kinds: Iterable
            The resource kinds, eg ["Deployment", "Namespace"]
        namespaces: Iterable
            Use None to download from all namespaces.

    Returns:
        Dict[MetaManifest, dict]: the K8s manifests from K8s.

    """
    # Output.
    server_manifests = {}

    # Ensure `namespaces` is always a list to avoid special casing below.
    if namespaces is None:
        namespaces = [None]

    # Download each resource type. Abort at the first error and return nothing.
    for namespace in namespaces:
        for kind in kinds:
            try:
                # Get the HTTP URL for the resource request.
                url, err = urlpath(config, kind, namespace)
                assert not err

                # Make HTTP request.
                manifest_list, err = k8s.get(client, url)
                assert not err

                # Parse the K8s List (eg DeploymentList, NamespaceList, ...) into a
                # Dict[MetaManifest, dict] dictionary.
                manifests, err = manio.unpack_list(manifest_list)
                assert not err

                # Drop all manifest fields except "apiVersion", "metadata" and "spec".
                ret = {k: manio.strip(config, man) for k, man in manifests.items()}
                manifests = {k: v.data for k, v in ret.items()}
                err = any((v.err for v in ret.values()))
                assert not err
            except AssertionError:
                # Return nothing, even if we had downloaded other kinds already.
                return RetVal(None, True)
            else:
                # Copy the manifests into the output dictionary.
                server_manifests.update(manifests)
    return RetVal(server_manifests, False)


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
            Usually the dictionary keys returned by `download_manifests`.

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
            Should be output from `download_manifests`.

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
        url, err = urlpath(config, delta.kind, namespace=delta.namespace)
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
        url, err = urlpath(config, meta.kind, namespace=meta.namespace)
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
        namespaces:
            Set to None to load all namespaces.

    Returns:
        None

    """
    try:
        local, err = manio.load(folder)
        assert not err
        server, err = download_manifests(config, client, kinds, namespaces)
        assert not err

        # Create the deployment plan.
        plan, err = compile_plan(config, local.meta, server)
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
        namespaces:
            Set to None to load all namespaces.

    Returns:
        None

    """
    try:
        # Load local and remote manifests.
        local, err = manio.load(folder)
        assert not err
        server, err = download_manifests(config, client, kinds, namespaces)
        assert not err

        # Create deployment plan.
        plan, err = compile_plan(config, local.meta, server)
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
        namespaces:
            Set to None to load all namespaces.

    Returns:
        None

    """
    try:
        local, err = manio.load(folder)
        assert not err

        server, err = download_manifests(config, client, kinds, namespaces)
        assert not err

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

    # Hard coded variables due to lacking command line support.
    manifest_folder = "manifests"
    kinds = SUPPORTED_KINDS
    namespace = None

    # Initialise logging.
    setup_logging(param.verbosity)

    # Create a `requests` client with proper security certificates to access
    # K8s API.
    kubeconf = os.path.expanduser('~/.kube/config')
    config = k8s.load_auto_config(kubeconf, disable_warnings=True)
    client = k8s.session(config)

    # Update the config with the correct K8s API version.
    config, err = k8s.version(config, client)
    if err:
        sys.exit(1)

    # Do what user asked us to do.
    if param.parser == "get":
        _, err = main_get(config, client, manifest_folder, kinds, namespace)
    elif param.parser == "diff":
        _, err = main_diff(config, client, manifest_folder, kinds, namespace)
    elif param.parser == "patch":
        _, err = main_patch(config, client, manifest_folder, kinds, namespace)
    else:
        logit.error(f"Unknown command <{param.parser}>")
        err = True

    # Abort with non-zero exit code if there was an error.
    if err:
        sys.exit(1)


if __name__ == '__main__':
    main()
