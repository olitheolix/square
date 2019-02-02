import re
import manio
import logging
import sys
import argparse
import collections
import copy
import difflib
import json
import yaml
import os
import textwrap
import jsonpatch
import k8s_utils as utils
from IPython import embed

import colorama
from pprint import pprint


# We support these resource types. The order matters because it determines the
# order in which the manifests will be grouped in the output files.
SUPPORTED_KINDS = ("Namespace", "Service", "Deployment")
SUPPORTED_VERSIONS = ("1.9", "1.10")

Patch = collections.namedtuple('Patch', 'url ops')
RetVal = collections.namedtuple('RetVal', 'data err')
DeploymentPlan = collections.namedtuple('DeploymentPlan', 'create patch delete')
MetaManifest = collections.namedtuple('MetaManifest', 'apiVersion kind namespace name')
DeltaPatch = collections.namedtuple("Delta", "meta diff patch")
DeltaCreate = collections.namedtuple("DeltaCreate", "meta url manifest")
DeltaDelete = collections.namedtuple("DeltaDelete", "meta url manifest")

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
    subparsers = parser.add_subparsers(help='Mode', dest='parser', title="Operation")

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


def diff_manifests(local: dict, server: dict):
    """Return the human readable diff between the `local` and `server`.

    The diff shows the necessary changes to transition the `server` manifest
    into the state of the `local` manifest.

    Inputs:
        local: dict
            Local manifest.
        server: dict
            Local manifest.

    Returns:
        str: human readable diff string as the Unix `diff` utility would
        produce it.

    """
    # Clean up the input manifests because we do not want to diff the eg
    # `status` fields.
    srv, err1 = manifest_metaspec(server)
    loc, err2 = manifest_metaspec(local)
    if err1 or err2:
        return RetVal(None, True)

    # Undo the DotDicts. This is a pre-caution because the YAML parser can
    # otherwise not dump the manifests.
    srv = utils.undo_dotdict(srv)
    loc = utils.undo_dotdict(loc)
    srv_lines = yaml.dump(srv, default_flow_style=False).splitlines()
    loc_lines = yaml.dump(loc, default_flow_style=False).splitlines()

    # Compute the diff.
    diff_lines = difflib.unified_diff(srv_lines, loc_lines, lineterm='')

    # Add some terminal colours to make it look prettier.
    out = []
    for line in diff_lines:
        if line.startswith('+'):
            out.append(colorama.Fore.GREEN + line + colorama.Fore.RESET)
        elif line.startswith('-'):
            out.append(colorama.Fore.RED + line + colorama.Fore.RESET)
        else:
            out.append(line)

    # Return the diff.
    return RetVal(str.join('\n', out), False)


def urlpath(config, kind, namespace):
    """Return complete URL to K8s resource.

    Inputs:
        config: k8s_utils.Config
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
        '1.9': {
            'Deployment': f'apis/extensions/v1beta1/{namespace}deployments',
            'Service': f'api/v1/{namespace}services',
            'Namespace': f'api/v1/namespaces',
        },
        '1.10': {
            'Deployment': f'apis/apps/v1/{namespace}deployments',
            'Service': f'api/v1/{namespace}services',
            'Namespace': f'api/v1/namespaces',
        },
    }

    # Look up the resource path and prefix it with the K8s API URL.
    path = resources[config.version][kind]
    url = f"{config.url}/{path}"
    return RetVal(url, False)


def compute_patch(config, local: dict, server: dict):
    """
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
    local, err1 = manifest_metaspec(local)
    server, err2 = manifest_metaspec(server)
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
    namespace = server.metadata.get("namespace", None)
    url, err = urlpath(config, server.kind, namespace)
    if err:
        return RetVal(None, True)
    full_url = f'{url}/{server.metadata.name}'

    # Compute JSON patch.
    patch = jsonpatch.make_patch(server, local)
    patch = json.loads(patch.to_string())

    # Return the patch.
    return RetVal(Patch(full_url, patch), False)


def manifest_metaspec(manifest: dict):
    """Return a copy of `manifest` with only the salient keys.

    The salient keys are: `apiVersion`, `kind`, `metadata` and `spec`.

    All other fields, eg `status` or `events` are irrelevant (and detrimental)
    when we compute diffs and patches. Only the fields mentioned above matter
    for that purpose.

    Inputs:
        manifest: dict

    Returns:
        dict: A strict subset of `manifest`.

    """
    # Avoid side effects for the caller. The DotDict improves the readability
    # of this function.
    manifest = utils.make_dotdict(copy.deepcopy(manifest))

    # Sanity check: `manifest` must have at least these fields in order to be
    # valid. Abort if it lacks one or more of them.
    must_have = {'apiVersion', 'kind', 'metadata', 'spec'}
    if not must_have.issubset(set(manifest.keys())):
        missing = must_have - set(manifest.keys())
        logit.error(f"Missing keys <{missing}> in manifest: <{manifest}>")
        return RetVal(None, True)
    del must_have

    # Return with an error if the manifest specifies an unsupported resource type.
    if manifest.kind not in SUPPORTED_KINDS:
        logit.error(f"Unsupported resource kind <{manifest.kind}> in <{manifest}>")
        return RetVal(None, True)

    # Manifests must also have certain keys in their metadata structure. Which
    # ones depends on whether or not it is a namespace manifest or not.
    if manifest.kind == "Namespace":
        if "namespace" in manifest.metadata:
            logit.error(
                f"Namespace must not have a `metadata.namespace` attribute: <{manifest}>"
            )
            return RetVal(None, True)
        must_have = {"name"}
    else:
        must_have = {"name", "namespace"}

    # Ensure the metadata field has the necessary fields.
    if not must_have.issubset(set(manifest.metadata.keys())):
        missing = must_have - set(manifest.metadata.keys())
        logit.error(f"Manifest metadata is missing these keys: {missing}: <{manifest}>")
        return RetVal(None, True)
    del must_have

    # Compile the salient metadata fields into a new dict.
    new_meta = {"name": manifest.metadata.name}
    if "namespace" in manifest.metadata:
        new_meta["namespace"] = manifest.metadata.namespace
    if "labels" in manifest.metadata:
        new_meta["labels"] = manifest.metadata.labels

    # Compile a new manifest with the salient keys only.
    ret = {
        "apiVersion": manifest.apiVersion,
        "kind": manifest.kind,
        "metadata": new_meta,
        "spec": manifest.spec,
    }
    return RetVal(utils.make_dotdict(ret), False)


def make_meta(manifest: dict):
    """Compile `MetaManifest` information from `manifest` and return it."""
    return MetaManifest(
        apiVersion=manifest['apiVersion'],
        kind=manifest['kind'],
        namespace=manifest['metadata'].get('namespace', None),
        name=manifest['metadata']['name']
    )


def list_parser(manifest_list: dict):
    """Unpack a K8s List item, eg `DeploymentList` or `NamespaceList`.

    Return a dictionary where each key uniquely identifies the resource via a
    `MetaManifest` tuple and the value is the actual JSON `manifest`.

    Input:
        manifest_list: dict
            K8s response from GET request for eg `deployments`.

    Returns:
        dict[MetaManifest:dict]

    """
    must_have = ("apiVersion", "kind", "items")
    missing = [key for key in must_have if key not in manifest_list]
    if len(missing) > 0:
        kind = manifest_list.get("kind", "UNKNOWN")
        logit.error(f"{kind} manifest is missing these keys: {missing}")
        return RetVal(None, True)
    del must_have, missing

    kind = manifest_list["kind"]
    if not kind.endswith('List'):
        logit.error(f"Kind {kind} is not a list")
        return RetVal(None, True)
    kind = kind[:-4]

    apiversion = manifest_list["apiVersion"]

    manifests = {}
    for manifest in manifest_list["items"]:
        manifest = copy.deepcopy(manifest)
        manifest['apiVersion'] = apiversion
        manifest['kind'] = kind

        manifests[make_meta(manifest)] = manifest
    return RetVal(manifests, False)


def k8s_request(client, method, path, payload, headers):
    """Return response of web request made with `client`.

    Inputs:
        client: `requests` session with correct K8s certificates.
        path: str
            Path to K8s resource (eg `/api/v1/namespaces`).
        payload: dict
            Anything that can be JSON encoded, usually a K8s manifest.
        headers: dict
            Request headers. These will *not* replace the existing request
            headers dictionary (eg the access tokens), but augment them.

    Returns:
        RetVal(dict, int): the JSON response and the HTTP status code.

    """
    try:
        ret = client.request(method, path, json=payload, headers=headers, timeout=30)
    except utils.requests.exceptions.ConnectionError as err:
        method = err.request.method
        url = err.request.url
        logit.error(f"Connection error: {method} {url}")
        return RetVal(None, True)

    try:
        response = ret.json()
    except json.decoder.JSONDecodeError as err:
        msg = (
            f"JSON error: {err.msg} in line {err.lineno} column {err.colno}",
            "-" * 80 + "\n" + err.doc + "\n" + "-" * 80,
        )
        logit.error(str.join("\n", msg))
        return RetVal(None, True)

    return RetVal(response, ret.status_code)


def k8s_delete(client, path: str, payload: dict):
    """Make DELETE requests to K8s (see `k8s_request`)."""
    resp, code = k8s_request(client, 'DELETE', path, payload, headers=None)
    return RetVal(resp, code != 200)


def k8s_get(client, path: str):
    """Make GET requests to K8s (see `k8s_request`)."""
    resp, code = k8s_request(client, 'GET', path, payload=None, headers=None)
    return RetVal(resp, code != 200)


def k8s_patch(client, path: str, payload: dict):
    """Make PATCH requests to K8s (see `k8s_request`)."""
    headers = {'Content-Type': 'application/json-patch+json'}
    resp, code = k8s_request(client, 'PATCH', path, payload, headers)
    return RetVal(resp, code != 200)


def k8s_post(client, path: str, payload: dict):
    """Make POST requests to K8s (see `k8s_request`)."""
    resp, code = k8s_request(client, 'POST', path, payload, headers=None)
    return RetVal(resp, code != 201)


def download_manifests(config, client, kinds, namespace):
    """Download and return the specified resource `kinds`.

    Set `namespace` to None to download from all namespaces.

    Either returns all the data or an error, never partial results.

    Inputs:
        config: k8s_utils.Config
        client: `requests` session with correct K8s certificates.
        kinds: Iterable
            The resource kinds, eg ["Deployment", "Namespace"]
        namespace: Iterable
            Use None to download from all namespaces.

    Returns:
        Dict[MetaManifest, dict]: the K8s manifests from K8s.

    """
    # Output.
    server_manifests = {}

    # Download each resource type. Abort at the first error and return nothing.
    for kind in kinds:
        try:
            # Get the HTTP URL for the resource request.
            url, err = urlpath(config, kind, namespace)
            assert not err

            # Make HTTP request.
            manifest_list, err = k8s_get(client, url)
            assert not err

            # Parse the K8s List (eg DeploymentList, NamespaceList, ...) into a
            # Dict[MetaManifest, dict] dictionary.
            manifests, err = list_parser(manifest_list)
            assert not err

            # Drop all manifest fields except "apiVersion", "metadata" and "spec".
            ret = {k: manifest_metaspec(man) for k, man in manifests.items()}
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
    # towards the setup specified in `local_manifests`.
    loc = set(local_manifests.keys())
    srv = set(server_manifests.keys())
    create = loc - srv
    patch = loc.intersection(srv)
    delete = srv - loc

    # We cannot use the sets because we need to preserve the relative
    # order of the elements in each group. Therefore, compile the stable output
    # into new lists and return those.
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
        config: k8s_utils.Config
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
    for meta in plan.create:
        url, err = urlpath(config, meta.kind, namespace=meta.namespace)
        if err:
            return RetVal(None, True)
        create.append(DeltaCreate(meta, url, local_manifests[meta]))

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

        # Assemble the delta and add it to the list.
        delete.append(DeltaDelete(meta, url, del_opts.copy()))

    # Iterate over each manifest that needs patching and determine the
    # necessary JSON Patch to transition K8s into the state specified in the
    # local manifests.
    patches = []
    for meta in plan.patch:
        loc = local_manifests[meta]
        srv = server_manifests[meta]

        # Compute textual diff (only useful for the user to study the diff).
        diff_str, err = diff_manifests(loc, srv)
        if err:
            return RetVal(None, True)

        # Compute the JSON patch that will match K8s to the local manifest.
        patch, err = compute_patch(config, loc, srv)
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
        if len(delta.diff) > 0:
            name = f'{delta.meta.namespace}/{delta.meta.name}'
            print('-' * 80 + '\n' + f'{name.upper()}\n' + '-' * 80)
            print(delta.diff)

    # Use Red to list all the resources that we should delete.
    for delta in plan.delete:
        name = f'--- {delta.meta.namespace}/{delta.meta.name} ---'
        print(cRed + name + cReset + "\n")

    return RetVal(None, False)


def get_k8s_version(config: utils.Config, client):
    """Return new `config` with version number of K8s API.

    Contact the K8s API, query its version via `client` and return `config`
    with an updated `version` field. All other field in `config` will remain
    intact.

    Inputs:
        config: k8s_utils.Config
        client: `requests` session with correct K8s certificates.

    Returns:
        k8s_utils.Config

    """
    # Ask the K8s API for its version and check for errors.
    url = f"{config.url}/version"
    ret = k8s_get(client, url)
    if ret.err:
        return ret

    # Construct the version number of the K8s API.
    major, minor = ret.data['major'], ret.data['minor']
    version = f"{major}.{minor}"

    # Return an updated `Config` tuple.
    config = config._replace(version=version)
    return RetVal(config, None)


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


def local_server_manifests(config, client, folder, kinds, namespace):
    """Return local-, server- and augmented local manifests.

    NOTE: This is a convenience wrapper around `manio.load`, `manio.unpack` and
    `download_manifest` to minimise code duplication.

    Inputs:
        config: k8s_utils.Config
        client: `requests` session with correct K8s certificates.
        folder: Path
            Path to local manifests eg "./foo"
        kinds: Iterable
            Resource types to fetch, eg ["Deployment", "Namespace"]
        namespace:
            Set to None to load all namespaces.

    Returns:
        (
            Dict[MetaManifest, dict],  # Local manifests.
            Dict[MetaManifest, dict],  # Server manifests.
            Dict[Filename, Tuple[MetaManifest, dict]]  # Local manifests with file names
        )

    """
    # Import local and server manifests.
    try:
        fdata_meta, err = manio.load(folder)
        assert not err

        local_manifests, err = manio.unpack(fdata_meta)
        assert not err

        server_manifests, err = download_manifests(config, client, kinds, namespace)
        assert not err
    except AssertionError:
        return RetVal(None, True)

    data = (local_manifests, server_manifests, fdata_meta)
    return RetVal(data, False)


def main_patch(config, client, folder, kinds, namespace):
    """Update K8s to match the specifications in `local_manifests`.

    Create a deployment plan that will transition the K8s state
    `server_manifests` to the desired `local_manifests`.

    Inputs:
        config: k8s_utils.Config
        client: `requests` session with correct K8s certificates.
        folder: Path
            Path to local manifests eg "./foo"
        kinds: Iterable
            Resource types to fetch, eg ["Deployment", "Namespace"]
        namespace:
            Set to None to load all namespaces.

    Returns:
        None

    """
    try:
        # Load local and remote manifests.
        data, err = local_server_manifests(config, client, folder, kinds, namespace)
        assert not err

        local_manifests, server_manifests, fdata_meta = data

        # Create the deployment plan.
        plan, err = compile_plan(config, local_manifests, server_manifests)
        assert not err

        # Present the plan confirm to go ahead with the user.
        print_deltas(plan)

        # Create the missing resources.
        for data in plan.create:
            print(f"Creating {data.meta.namespace}/{data.meta.name}")
            _, err = k8s_post(client, data.url, data.manifest)
            assert not err

        # Patch the server resources.
        patches = [_.patch for _ in plan.patch if len(_.patch.ops) > 0]
        print(f"Compiled {len(patches)} patches.")
        for patch in patches:
            pprint(patch)
            _, err = k8s_patch(client, patch.url, patch.ops)
            assert not err

        # Delete the excess resources.
        for data in plan.delete:
            print(f"Deleting {data.meta.namespace}/{data.meta.name}")
            _, err = k8s_delete(client, data.url, data.manifest)
            assert not err
    except AssertionError:
        return RetVal(None, True)

    # All good.
    return RetVal(None, False)


def main_diff(config, client, folder, kinds, namespace):
    """Print the diff between `local_manifests` and `server_manifests`.

    The diff shows what would have to change on the K8s server in order for it
    to match the setup defined in `local_manifests`.

    Inputs:
        config: k8s_utils.Config
        client: `requests` session with correct K8s certificates.
        folder: Path
            Path to local manifests eg "./foo"
        kinds: Iterable
            Resource types to fetch, eg ["Deployment", "Namespace"]
        namespace:
            Set to None to load all namespaces.

    Returns:
        None

    """
    try:
        # Load local and remote manifests.
        data, err = local_server_manifests(config, client, folder, kinds, namespace)
        assert not err
        local_manifests, server_manifests, fdata_meta = data

        # Create deployment plan.
        plan, err = compile_plan(config, local_manifests, server_manifests)
        assert not err
    except AssertionError:
        return RetVal(None, True)

    # Print the plan and return.
    print_deltas(plan)
    return RetVal(None, False)


def main_get(config, client, folder, kinds, namespace):
    """Download all K8s manifests and merge them into local files.

    Inputs:
        config: k8s_utils.Config
        client: `requests` session with correct K8s certificates.
        folder: Path
            Path to local manifests eg "./foo"
        kinds: Iterable
            Resource types to fetch, eg ["Deployment", "Namespace"]
        namespace:
            Set to None to load all namespaces.

    Returns:
        None

    """
    try:
        # Load local and remote manifests.
        data, err = local_server_manifests(config, client, folder, kinds, namespace)
        assert not err
        local_manifests, server_manifests, fdata_meta = data

        # Sync the server manifests into the local manfifests. All this happens in
        # memory and no files will be modified here - see next step.
        synced_manifests, err = manio.sync(fdata_meta, server_manifests)
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
    config = utils.load_auto_config(kubeconf, disable_warnings=True)
    client = utils.setup_requests(config)

    # Update the config with the correct K8s API version.
    config, _ = get_k8s_version(config, client)

    # Do what user asked us to do.
    if param.parser == "get":
        _, err = main_get(config, client, manifest_folder, kinds, namespace)
    elif param.parser == "diff":
        _, err = main_diff(config, client, manifest_folder, kinds, namespace)
    elif param.parser == "patch":
        _, err = main_patch(config, client, manifest_folder, kinds, namespace)
    else:
        print(f"Unknown command <{param.parser}>")
        sys.exit(1)


if __name__ == '__main__':
    main()
