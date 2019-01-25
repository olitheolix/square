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


Patch = collections.namedtuple('Patch', 'url ops')
RetVal = collections.namedtuple('RetVal', 'data err')
DeploymentPlan = collections.namedtuple('DeploymentPlan', 'create patch delete')
ManifestMeta = collections.namedtuple('ManifestMeta', 'apiVersion kind namespace name')
Delta = collections.namedtuple("Delta", "namespace name diff patch")
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
    parent.add_argument(
        '--log-level', default='info',
        choices=('debug', 'info', 'warning', 'error'),
        help="Debug level"
    )

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


def diff_manifests(src: dict, dst: dict):
    src, err = manifest_metaspec(src)
    if err:
        return RetVal(None, True)
    dst, err = manifest_metaspec(dst)
    if err:
        return RetVal(None, True)

    src_lines = yaml.dump(src, default_flow_style=False).splitlines()
    dst_lines = yaml.dump(dst, default_flow_style=False).splitlines()

    diff_lines = difflib.unified_diff(src_lines, dst_lines, lineterm='')

    out = []
    for line in diff_lines:
        if line.startswith('+'):
            out.append(colorama.Fore.GREEN + line + colorama.Fore.RESET)
        elif line.startswith('-'):
            out.append(colorama.Fore.RED + line + colorama.Fore.RESET)
        elif line.startswith('^'):
            out.append(colorama.Fore.BLUE + line + colorama.Fore.RESET)
        else:
            out.append(line)

    return RetVal(str.join('\n', out), None)


def resource_url(config, resource, namespace):
    _ns_ = '' if namespace is None else f'namespaces/{namespace}/'
    resource = resource.lower()

    resources = {
        '1.9': {
            'deployment': f'apis/extensions/v1beta1/{_ns_}deployments',
            'service': f'api/v1/{_ns_}services',
            'namespace': f'api/v1/namespaces',
        },
        '1.10': {
            'deployment': f'apis/apps/v1/{_ns_}deployments',
            'service': f'api/v1/{_ns_}services',
            'namespace': f'api/v1/namespaces',
        },
    }

    path = resources[config.version][resource]
    return f'{config.url}/{path}'


def compute_patch(config, src, dst):
    # fixme: rename src/dst to loc/srv?
    src, err = manifest_metaspec(src)
    if err:
        return RetVal(None, True)
    dst, err = manifest_metaspec(dst)
    if err:
        return RetVal(None, True)

    try:
        assert src["apiVersion"] == dst["apiVersion"]
        assert src["kind"] == dst["kind"]
        assert src["metadata"]["name"] == dst["metadata"]["name"]
        if src["kind"] != "Namespace":
            assert src["metadata"]["namespace"] == dst["metadata"]["namespace"]
    except AssertionError:
        logit.error("Cannot compute JSON patch for incompatible manifests")
        return RetVal(None, True)

    kind = src['kind']
    name = src['metadata']['name']
    namespace = src['metadata'].get('namespace', None)

    url = resource_url(config, kind, namespace)

    patch = jsonpatch.make_patch(src, dst)
    patch = json.loads(patch.to_string())
    full_url = f'{url}/{name}'

    return RetVal(Patch(full_url, patch), None)


def manifest_metaspec(manifest: dict):
    manifest = copy.deepcopy(manifest)

    must_have = {'apiVersion', 'kind', 'metadata', 'spec'}
    if not must_have.issubset(set(manifest.keys())):
        missing = must_have - set(manifest.keys())
        logit.error(f"Manifest is missing these keys: {missing}")
        return RetVal(None, True)
    del must_have

    if manifest["kind"] == "Namespace":
        if "namespace" in manifest["metadata"]:
            logit.error("Namespace must not have a `metadata.namespace` attribute")
            return RetVal(None, True)
        must_have = {"name"}
    else:
        must_have = {"name", "namespace"}

    if not must_have.issubset(set(manifest["metadata"].keys())):
        missing = must_have - set(manifest["metadata"].keys())
        logit.error(f"Manifest metadata is missing these keys: {missing}")
        return RetVal(None, True)

    if manifest["kind"].lower().capitalize() != manifest["kind"]:
        logit.error(f"Invalid capitalisation: {manifest['kind']}")
        return RetVal(None, True)

    old_meta = manifest['metadata']
    new_meta = {'name': old_meta['name']}
    if 'namespace' in old_meta:
        new_meta['namespace'] = old_meta['namespace']
    if 'labels' in old_meta:
        new_meta['labels'] = old_meta['labels']

    ret = {
        'apiVersion': manifest['apiVersion'],
        'kind': manifest['kind'],
        'metadata': new_meta,
        'spec': manifest['spec'],
    }
    return RetVal(ret, None)


def list_parser(manifest_list: dict):
    """Unpack a K8s List item, eg `DeploymentList` or `NamespaceList`.

    Return a dictionary where each key uniquely identifies the resource via a
    `ManifestMeta` tuple and the value is the actual JSON `manifest`.

    Input:
        manifest_list: dict
            K8s response from GET request for eg `deployments`.

    Returns:
        dict[ManifestMeta:dict]

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
    meta_ref = ManifestMeta(apiversion, kind, None, None)

    manifests = {}
    for manifest in manifest_list["items"]:
        manifest = copy.deepcopy(manifest)
        ns = manifest['metadata'].get('namespace', None)
        name = manifest['metadata']['name']
        key = meta_ref._replace(namespace=ns, name=name)

        manifests[key] = manifest
        manifests[key]['apiVersion'] = apiversion
        manifests[key]['kind'] = kind
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
        # fixme: log
        method = err.request.method
        url = err.request.url
        print(f"Connection error: {method} {url}")
        return RetVal(None, True)

    try:
        response = ret.json()
    except json.decoder.JSONDecodeError as err:
        # fixme: log
        print(f"JSON error: {err.msg} in line {err.lineno} column {err.colno}")
        print("-" * 80 + "\n" + err.doc + "\n" + "-" * 80)
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
    server_manifests = {}
    for kind in kinds:
        url = resource_url(config, kind, namespace=namespace)
        manifest_list, _ = k8s_get(client, url)
        manifests, _ = list_parser(manifest_list)
        manifests = {k: manifest_metaspec(man)[0] for k, man in manifests.items()}
        server_manifests.update(manifests)
    return RetVal(server_manifests, None)


def diffpatch(config, local_manifests, server_manifests):
    plan, _ = compute_plan(local_manifests, server_manifests)

    create = []
    for meta in plan.create:
        url = resource_url(config, meta.kind, namespace=meta.namespace)
        create.append(DeltaCreate(meta, url, local_manifests[meta]))

    delete = []
    del_opts = {
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "gracePeriodSeconds": 0,
        "orphanDependents": False,
    }
    for meta in plan.delete:
        url = resource_url(config, meta.kind, namespace=meta.namespace)
        url = f"{url}/{meta.name}"
        delete.append(DeltaDelete(meta, url, copy.deepcopy(del_opts)))

    patches = []
    for meta in plan.patch:
        name = f'{meta.namespace}/{meta.name}'
        try:
            local = local_manifests[meta]
            remote = server_manifests[meta]
        except KeyError:
            # fixme: ensure beforehand that keys exist and compile the list of
            # resources to add/delete on the server.
            print(f'Mismatch for ingress {name}')
            continue

        # Compute textual diff (only useful for the user to study the diff).
        diff_str, err = diff_manifests(remote, local)
        if err:
            return RetVal(None, True)

        patch, err = compute_patch(config, remote, local)
        if err:
            return RetVal(None, True)
        patches.append(Delta(meta.namespace, meta.name, diff_str, patch))

    new_plan = DeploymentPlan(create, patches, delete)
    return RetVal(new_plan, None)


def print_deltas(plan):
    cGreen = colorama.Fore.GREEN
    cRed = colorama.Fore.RED
    cReset = colorama.Fore.RESET

    for delta in plan.create:
        name = f'{delta.meta.namespace}/{delta.meta.name}'
        txt = yaml.dump(delta.manifest, default_flow_style=False)
        txt = [cGreen + line + cReset for line in txt.splitlines()]
        txt.insert(0, cGreen + f"--- {name} ---" + cReset)
        txt = str.join('\n', txt)
        print(txt + '\n')

    deltas = plan.patch
    for delta in deltas:
        if len(delta.diff) > 0:
            name = f'{delta.namespace}/{delta.name}'
            print('-' * 80 + '\n' + f'{name.upper()}\n' + '-' * 80)
            print(delta.diff)

    for delta in plan.delete:
        name = f'--- {delta.meta.namespace}/{delta.meta.name} ---'
        print(cRed + name + cReset + "\n")

    return RetVal(None, None)


def load_manifest(fname):
    raw = yaml.safe_load_all(open(fname))
    manifests = {}

    for manifest in raw:
        key = ManifestMeta(
            apiVersion=manifest['apiVersion'],
            kind=manifest['kind'],
            namespace=manifest['metadata'].get('namespace', None),
            name=manifest['metadata']['name'],
        )
        manifests[key] = copy.deepcopy(manifest)
    return manifests


def save_manifests(manifests, fname):
    yaml.safe_dump_all(
        manifests.values(),
        open(fname, 'w'),
        default_flow_style=False,
    )


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
        meta_manifests: Iterable[ManifestMeta]

    Returns:
        set[ManifestMeta]: orphaned resources.

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


def compute_plan(local_manifests, server_manifests):
    """Return the `DeploymentPlan` to transition K8s to state of `local_manifests`.

    The deployment plan is a named tuple. It specifies which resources to
    create, patch and delete to ensure that the K8s state (specified by
    `server_manifests` usually returned by `download_manifests`) will match the
    state specified in the `local_manifests`.

    The returned deployment plan will contain *every resource in
    `local_manifests` and `server_manifests` exactly once*.

    Create: all resources that exist in `local_manifests` but not in `server_manifests`.
    Delete: all resources that exist in `server_manifests` but not in `local_manifests`.
    Patch : all resources that exist in both.

    NOTE: resources that exist locally and on the server *may* be identical and
    would thus not need a patch. They are nevertheless added to the `patched`
    attribute of the deployment. It is the job of the function doing the actual
    patching to figure out the difference, if any.

    Inputs:
        local_manifests: set[ManifestMeta]
            Usually the dictionary keys returned by `load_manifest`.
        server_manifests: set[ManifestMeta]
            Usually the dictionary keys returned by `download_manifests`.

    Returns:
        DeploymentPlan

    """
    # Ensure the input is a genuine set and remove all duplicates.
    local_manifests = set(local_manifests)
    server_manifests = set(server_manifests)

    # Determine what needs adding, removing and patching to steer the K8s setup
    # towards the setup specified in `local_manifests`.
    create = local_manifests - server_manifests
    patch = local_manifests.intersection(server_manifests)
    delete = server_manifests - local_manifests

    # Return the deployment plan.
    plan = DeploymentPlan(create, patch, delete)
    return RetVal(plan, None)


def setup_logging(level: str):
    """Configure logging at `level`.

    Inputs:
        level: str
            One of ('debug', 'info', 'warn'). Not case sensitive.

    Returns:
        None

    """
    # Pick the correct log level.
    if level.upper() == "DEBUG":
        level = logging.DEBUG
    elif level.upper() == "INFO":
        level = logging.INFO
    elif level.upper() == "WARN":
        level = logging.WARN
    else:
        print(f"Unknown log level {level} - abort")
        sys.exit(1)

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


def main():
    param = parse_commandline_args()

    # Initialise logging.
    setup_logging("DEBUG")

    # Create a `requests` client with proper security certificates to access
    # K8s API.
    kubeconf = os.path.expanduser('~/.kube/config')
    config = utils.load_auto_config(kubeconf, disable_warnings=True)
    client = utils.setup_requests(config)

    # Update the config with the correct K8s API version.
    config, _ = get_k8s_version(config, client)

    # Hard coded variables due to lacking of command line support.
    fname = '/tmp/manifests.yaml'
    kinds = ('namespace', 'service', 'deployment')
    namespace = None

    if param.parser == "get":
        server_manifests, _ = download_manifests(config, client, kinds, namespace)
        save_manifests(server_manifests, fname)
    elif param.parser == "diff":
        local_manifests = load_manifest(fname)
        server_manifests, _ = download_manifests(config, client, kinds, namespace)
        plan, err = diffpatch(config, local_manifests, server_manifests)
        print_deltas(plan)
    elif param.parser == "patch":
        local_manifests = load_manifest(fname)
        server_manifests, _ = download_manifests(config, client, kinds, namespace)

        plan, err = diffpatch(config, local_manifests, server_manifests)
        print_deltas(plan)

        for data in plan.create:
            print(f"Creating {data.meta.namespace}/{data.meta.name}")
            ret = k8s_post(client, data.url, data.manifest)
            if ret.err:
                print(ret)
                return

        patches = [_.patch for _ in plan.patch if len(_.patch.ops) > 0]
        print(f"Compiled {len(patches)} patches.")
        for patch in patches:
            pprint(patch)
            ret = k8s_patch(client, patch.url, patch.ops)
            if ret.err:
                print(ret)
                return

        for data in plan.delete:
            print(f"Deleting {data.meta.namespace}/{data.meta.name}")
            ret = k8s_delete(client, data.url, data.manifest)
            if ret.err:
                print(ret)
                return
    else:
        print(f"Unknown command <{param.parser}>")
        sys.exit(1)


if __name__ == '__main__':
    main()
