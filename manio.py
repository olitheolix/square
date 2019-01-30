import glob
import os
import copy
import collections

import square
import yaml
import k8s_utils

from square import RetVal

# Convenience: global logger instance to avoid repetitive code.
logit = square.logging.getLogger("square")


def parse(file_yaml: dict):
    """Parse all YAML strings in `file_yaml` and return result.

    Inputs:
        file_yaml: Dict[Filename, str]
            Raw data as returned by `load_files`.

    Returns:
        Dict[Filename, Tuple(MetaManifest, dict)]: YAML parsed manifests in
        each file.

    """
    # The output dict will have a list of tuples.
    out = {}

    # Parse the YAML documents from every file.
    for fname, yaml_str in file_yaml.items():
        logit.debug(f"Parsing <{fname}>")

        # Decode the YAML documents in the current file.
        try:
            manifests = list(yaml.safe_load_all(yaml_str))
        except yaml.scanner.ScannerError as err:
            logit.error(
                f"Cannot YAML parse <{fname}>"
                f" - {err.problem} - Line {err.problem_mark.line}"
            )
            return RetVal(None, True)

        # Convert List[manifest] into List[(MetaManifest, manifest)].
        out[fname] = [(square.make_meta(_), _) for _ in manifests]

    # Drop all files without manifests.
    out = {k: v for k, v in out.items() if len(v) > 0}
    num_manifests = [len(_) for _ in out.values()]
    logit.debug(f"Parsed {sum(num_manifests)} in {len(num_manifests)} files")

    # Return the YAML parsed manifests.
    return RetVal(out, False)


def unpack(data: dict):
    """Drop the "Filename" dimension from `data`.

    Returns an error unless all resources are unique. For instance, return an
    error if two files define the same namespace or the same deployment.

    Inputs:
        data: Dict[Filename, Tuple[MetaManifest, dict]]

    Returns:
        Dict[MetaManifest, dict]: flattened version of `data`.

    """
    # Compile a dict that shows which meta manifest was defined in which file.
    # We will use this information short to determine if any resources were
    # specified multiple times in either the same or different file.
    all_meta = collections.defaultdict(list)
    for fname in data:
        for meta, _ in data[fname]:
            all_meta[meta].append(fname)

    # Find out if all meta manifests were unique. If not, log the culprits and
    # return with an error.
    is_unique = True
    for meta, fnames in all_meta.items():
        if len(fnames) > 1:
            is_unique = False
            logit.error(
                f"Meta manifest {meta} was defined {len(fnames)} times: "
                f"{str.join(', ', fnames)}"
            )
    if not is_unique:
        return RetVal(None, True)

    # Compile the input data into a new dict with the meta manifest as key.
    out = {k: v for fname in data for k, v in data[fname]}
    return RetVal(out, False)


def sync(local_manifests, server_manifests):
    """Update the local manifests with the server values and return the result.

    Inputs:
        local_manifests: Dict[Filename, Tuple[MetaManifest, dict]]
        server_manifests: Dict[MetaManifest, dict]

    Returns:
        Dict[Filename, Tuple[MetaManifest, dict]]

    """
    # Create a dict to maps a given MetaManifest to the file which defined it.
    # Do not only store the file but also the index of the YAML manifest inside
    # the file. We will need that information later to find out which manifest
    # in which file we need to update.
    meta_to_fname = {}
    for fname in local_manifests:
        for idx, (meta, _) in enumerate(local_manifests[fname]):
            meta_to_fname[meta] = (fname, idx)
            del meta
        del fname

    # Make a copy of the local manifests to avoid side effects for the caller.
    # We can then safely overwrite the local manifests with the server ones.
    out_add_mod = copy.deepcopy(local_manifests)
    del local_manifests

    # Make sure the dict has a "default.yaml" file.
    if "default.yaml" not in out_add_mod:
        out_add_mod["default.yaml"] = []

    # If the meta manifest from the server also exists locally then update the
    # respective manifest, otherwise add it to "default.yaml".
    for meta, manifest in server_manifests.items():
        try:
            # Find out the YAML document index and file that defined `meta`.
            fname, idx = meta_to_fname[meta]
        except KeyError:
            # Append the manifest to the catch-all file "default.yaml" because
            # it does not exists locally anywhere.
            out_add_mod["default.yaml"].append((meta, manifest))
        else:
            # Update the correct YAML document in the correct file.
            out_add_mod[fname][idx] = (meta, manifest)

    # Delete the meta manifests that do not exist on the server. To do just
    # that, iterate over all meta manifests in all files and find out if we
    # have that meta manifest locally anywhere. Only include that manifest in
    # the new output if we do, otherwise skip it to, in effect, delete it.
    out_add_mod_del = {}
    for fname, manifests in out_add_mod.items():
        pruned = [(meta, man) for (meta, man) in manifests if meta in server_manifests]
        out_add_mod_del[fname] = pruned

    return RetVal(out_add_mod_del, False)


def unparse(file_manifests):
    out = {
        fname: [manifest for _, manifest in manifests]
        for fname, manifests in file_manifests.items()
    }
    out = {k: v for k, v in out.items() if len(v) > 0}
    out = {k: k8s_utils.undo_dotdict(v) for k, v in out.items()}
    out = {k: yaml.safe_dump_all(v, default_flow_style=False) for k, v in out.items()}
    return RetVal(out, False)


def save_files(folder, data: dict):
    for fname, yaml_str in data.items():
        fname = os.path.join(folder, fname)
        path, _ = os.path.split(fname)
        logit.debug(f"Creating path for <{fname}>")
        os.makedirs(path, exist_ok=True)
        logit.debug(f"Saving YAML file <{fname}>")
        open(fname, 'w').write(yaml_str)
    return RetVal(None, False)


def load_files(folder, fnames: str):
    out = {}
    for fname_rel in fnames:
        fname_abs = os.path.join(folder, fname_rel)
        logit.debug(f"Loading {fname_abs}")
        out[fname_rel] = open(fname_abs, "r").read()
    return RetVal(out, False)


def load(folder):
    fnames = glob.glob(os.path.join(folder, "**", "*.yaml"), recursive=True)
    fnames = [_[len(folder) + 1:] for _ in fnames]
    fdata_raw, err = load_files(folder, fnames)
    return parse(fdata_raw)


def save(folder, manifests: dict):
    fdata_raw, err = unparse(manifests)
    return save_files(folder, fdata_raw)
