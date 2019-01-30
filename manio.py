import glob
import os
import copy

import square
import yaml

from square import RetVal


def parse(data: dict):
    out = {}
    for fname, yaml_str in data.items():
        manifests = yaml.safe_load_all(yaml_str)
        out[fname] = []
        for manifest in manifests:
            key = square.make_meta(manifest)
            out[fname].append((key, manifest))
    return RetVal(out, False)


def unpack(data: dict):
    out = {k: v for fname in data for k, v in data[fname]}
    return RetVal(out, False)


def sync(local_manifests, server_manifests):
    meta_to_fname = {}
    for fname in local_manifests:
        for idx, (meta, _) in enumerate(local_manifests[fname]):
            meta_to_fname[meta] = (fname, idx)
            del meta
        del fname

    # Make a copy of the local manifests so we can safely overwrite the local
    # manifests with the server ones.
    out = copy.deepcopy(local_manifests)
    out["default.yaml"] = []
    for meta, manifest in server_manifests.items():
        try:
            fname, idx = meta_to_fname[meta]
            out[fname][idx] = (meta, manifest)
        except KeyError:
            out["default.yaml"].append((meta, manifest))

    # Remove all the manifests that exist locally but not on the server
    # anymore.
    out2 = {}
    for fname, manifests in out.items():
        pruned = [_ for _ in manifests if _[0] in server_manifests]
        out2[fname] = pruned

    return RetVal(out2, False)


def unparse(file_manifests):
    out = {
        fname: [manifest for _, manifest in manifests]
        for fname, manifests in file_manifests.items()
    }
    out = {k: v for k, v in out.items() if len(v) > 0}
    out = {k: yaml.safe_dump_all(v) for k, v in out.items()}
    return RetVal(out, False)


def save(data: dict):
    for fname, yaml_str in data.items():
        path, _ = os.path.split(fname)
        os.makedirs(path, exist_ok=True)
        open(fname, 'w').write(yaml_str)
    return RetVal(None, False)


def load(fnames: str):
    out = {}
    for fname in fnames:
        assert os.path.exists(fname)
        out[fname] = open(fname, "r").read()
    return RetVal(out, False)
