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
DeploymentPlan = collections.namedtuple('DeploymentPlan', 'add rem mod')
ManifestMeta = collections.namedtuple('ManifestMeta', 'apiVersion kind namespace name')
Delta = collections.namedtuple("Delta", "namespace name diff patch")


def parse_commandline_args():
    """Return parsed command line."""
    name = os.path.basename(__file__)
    description = textwrap.dedent(f'''
    Manage Kubernetes manifests.

    Examples:
      {name} fetch
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

    # Sub-command FETCH.
    parser_fetch = subparsers.add_parser(
        'fetch', help="Fetch manifests", parents=[parent]
    )
    padd = parser_fetch.add_argument
    padd('kind', type=str, nargs='*')
    del parser_fetch, padd

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
        return RetVal(None, err)
    dst, err = manifest_metaspec(dst)
    if err:
        return RetVal(None, err)

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


def resource_url(config, k8s_version, resource, namespace):
    _ns_ = '' if namespace is None else f'namespaces/{namespace}/'
    resource = resource.lower()

    resources = {
        '1.9': {
            'deployment': f'apis/extensions/v1beta1/{_ns_}deployments',
            'service': f'api/v1/{_ns_}services',
            'namespace': f'api/v1/namespaces',
        },
        '1.11': {
            'deployment': f'apis/apps/v1/{_ns_}deployments',
            'service': f'api/v1/{_ns_}services',
            'namespace': f'api/v1/namespaces',
        },
    }

    path = resources[k8s_version][resource]
    return f'{config.url}/{path}'


def compute_patch(config, k8s_version, src, dst):
    src, err = manifest_metaspec(src)
    if err:
        return RetVal(None, err)
    dst, err = manifest_metaspec(dst)
    if err:
        return RetVal(None, err)

    try:
        assert src["apiVersion"] == dst["apiVersion"]
        assert src["kind"] == dst["kind"]
        assert src["metadata"]["name"] == dst["metadata"]["name"]
        if src["kind"] != "Namespace":
            assert src["metadata"]["namespace"] == dst["metadata"]["namespace"]
    except AssertionError:
        # fixme: log message
        return RetVal(None, "Cannot compute JSON patch for incompatible manifests")

    kind = src['kind']
    name = src['metadata']['name']
    namespace = src['metadata'].get('namespace', None)

    url = resource_url(config, k8s_version, kind, namespace)

    patch = jsonpatch.make_patch(src, dst)
    patch = json.loads(patch.to_string())
    full_url = f'{url}/{name}'

    return RetVal(Patch(full_url, patch), None)


def k8s_patch(client, full_url, json_patch):
    headers = {'Content-Type': 'application/json-patch+json'}

    try:
        ret = client.patch(full_url, json=json_patch, headers=headers)
    except utils.requests.exceptions.ConnectionError:
        # fixme: log
        # fixme: json encoding error
        return RetVal(None, "Connection error")

    if ret.status_code != 200:
        return RetVal(None, "K8s operation failed")

    return RetVal(None, None)


def manifest_metaspec(manifest: dict):
    manifest = copy.deepcopy(manifest)

    must_have = {'apiVersion', 'kind', 'metadata', 'spec'}
    if not must_have.issubset(set(manifest.keys())):
        # fixme: log which attributes are missing.
        return RetVal(None, "Manifest is missing attributes")

    if manifest["kind"] == "Namespace":
        if "namespace" in manifest["metadata"]:
            return RetVal(
                None,
                "Namespace manifest must not have metadata.namespace attribute"
            )
        must_have = {"name"}
    else:
        must_have = {"name", "namespace"}

    if not must_have.issubset(set(manifest["metadata"].keys())):
        # fixme: log which attributes are missing.
        return RetVal(None, "Manifest metadata is missing attributes")

    if manifest["kind"].lower().capitalize() != manifest["kind"]:
        return RetVal(None, "<kind> attribute must be capitalised")

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


def _k8s_get_list_parser(manifest_list: dict):
    # fixme: rename function to just `list_parser`.
    try:
        apiversion = manifest_list['apiVersion']
        kind = manifest_list['kind']
        items = manifest_list['items']
    except KeyError as err:
        # fixme: log which key is missing
        return RetVal(None, 'Invalid K8s List resource')

    if not kind.endswith('List'):
        # fixme: log name of invalid kind
        return RetVal(None, 'Invalid K8s List resource')

    kind = kind[:-4]
    meta_ref = ManifestMeta(apiversion, kind, None, None)

    manifests = {}
    for manifest in items:
        manifest = copy.deepcopy(manifest)
        ns = manifest['metadata'].get('namespace', None)
        name = manifest['metadata']['name']
        key = meta_ref._replace(namespace=ns, name=name)

        manifests[key] = manifest
        manifests[key]['apiVersion'] = apiversion
        manifests[key]['kind'] = kind
    return RetVal(manifests, None)


def k8s_get_list(client, config, k8s_version, resource, namespace):
    url = resource_url(config, k8s_version, resource, namespace)

    try:
        ret = client.get(url)
    except utils.requests.exceptions.ConnectionError:
        # fixme: log
        return RetVal(None, "Connection error")

    if not 200 <= ret.status_code < 300:
        # fixme: log
        return RetVal(None, "K8s responded with error")

    try:
        response = ret.json()
    except json.decoder.JSONDecodeError:
        # fixme: log
        return RetVal(None, "K8s returned corrupt JSON")

    return _k8s_get_list_parser(response)


def diffpatch(config, client, k8s_version, kinds, fname):
    server_manifests, _ = k8s_fetch(config, client, k8s_version, kinds)
    local_manifests = load_manifest(fname)

    out = []
    for meta in local_manifests:
        name = f'{meta.namespace}/{meta.name}'
        try:
            local = local_manifests[meta]
            remote = server_manifests[meta]
        except KeyError:
            # fixme: ensure beforehand that keys exist and compile the list of
            # resources to add/delete on the server.
            print(f'Mismatch for ingress {name}')
            continue

        diff, err = diff_manifests(remote, local)
        if err:
            # fixme: log
            return RetVal(None, err)

        patch, err = compute_patch(config, k8s_version, remote, local)
        if err:
            # fixme: log
            return RetVal(None, err)
        out.append(Delta(meta.namespace, meta.name, diff, patch))
    return RetVal(out, None)


def print_deltas(deltas):
    for delta in deltas:
        if len(delta.diff) > 0:
            name = f'{delta.namespace}/{delta.name}'
            print('-' * 80 + '\n' + f'{name.upper()}\n' + '-' * 80)
            print(delta.diff)
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


def k8s_fetch(config, client, k8s_version, kinds):
    server_manifests = {}
    for kind in kinds:
        manifests, _ = k8s_get_list(client, config, k8s_version, kind, None)
        manifests = {k: manifest_metaspec(man)[0] for k, man in manifests.items()}
        server_manifests.update(manifests)
    return RetVal(server_manifests, None)


def save_manifests(manifests, fname):
    yaml.safe_dump_all(
        manifests.values(),
        open(fname, 'w'),
        default_flow_style=False,
    )


def main():
    param = parse_commandline_args()

    kubeconf = os.path.expanduser('~/.kube/config')
    config = utils.load_auto_config(kubeconf, disable_warnings=True)
    client = utils.setup_requests(config)
    k8s_version = '1.11'
    fname = '/tmp/manifests.yaml'

    kinds = ('namespace', 'service', 'deployment')

    if param.parser == "fetch":
        server_manifests, _ = k8s_fetch(config, client, k8s_version, kinds)
        save_manifests(server_manifests, fname)
    elif param.parser == "diff":
        deltas, err = diffpatch(config, client, k8s_version, kinds, fname)
        print_deltas(deltas)
    elif param.parser == "patch":
        deltas, err = diffpatch(config, client, k8s_version, kinds, fname)
        print_deltas(deltas)

        patches = [_.patch for _ in deltas if len(_.patch.ops) > 0]
        print(f"Compiled {len(patches)} patches.")
        for patch in patches:
            pprint(patch)
            ret = k8s_patch(client, patch.url, patch.ops)
            if ret.err:
                print(ret)
                return
    else:
        print(f"Unknown command <{param.parser}>")
        sys.exit(1)


if __name__ == '__main__':
    main()
