from typing import Any, Dict


def make_manifest(kind: str, namespace: str, name: str) -> dict:
    manifest: Dict[str, Any]
    manifest = {
        'apiVersion': 'v1',
        'kind': kind,
        'metadata': {
            'name': name,
            'labels': {'key': 'val'},
            'foo': 'bar',
        },
        'spec': {
            'finalizers': ['kubernetes']
        },
        'status': {
            'some': 'status',
        },
        'garbage': 'more garbage',
    }

    # Only create namespace entry if one was specified.
    if namespace is not None:
        manifest['metadata']['namespace'] = namespace

    return manifest


def mk_deploy(name: str, ns: str = "namespace") -> dict:
    return make_manifest("Deployment", ns, name)
