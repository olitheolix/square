import collections
import copy
import difflib
import logging
import pathlib

import dotdict
import schemas
import yaml
from dtypes import SUPPORTED_KINDS, MetaManifest, RetVal

# Convenience: global logger instance to avoid repetitive code.
logit = logging.getLogger("square")


def make_meta(manifest: dict):
    """Compile `MetaManifest` information from `manifest` and return it."""
    # Unpack the namespace. For Namespace resources, this will be the "name".
    if manifest["kind"] == "Namespace":
        ns = manifest['metadata']['name']
    else:
        ns = manifest['metadata']['namespace']

    # Return the populated MetaManifest.
    return MetaManifest(
        apiVersion=manifest['apiVersion'],
        kind=manifest['kind'],
        namespace=ns,
        name=manifest['metadata']['name']
    )


def unpack_list(manifest_list: dict):
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
        out[fname] = [(make_meta(_), _) for _ in manifests]

    # Drop all files without manifests.
    out = {k: v for k, v in out.items() if len(v) > 0}
    num_manifests = [len(_) for _ in out.values()]
    logit.debug(f"Parsed {sum(num_manifests)} manifests in {len(num_manifests)} files")

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


def sync(local_manifests, server_manifests, kinds, namespaces):
    """Update the local manifests with the server values and return the result.

    Inputs:
        local_manifests: Dict[Filename, Tuple[MetaManifest, dict]]
        server_manifests: Dict[MetaManifest, dict]
        kinds: Iterable
            Resource types to fetch, eg ["Deployment", "Namespace"]
        namespaces: Iterable
            Set to None to load all namespaces.

    Returns:
        Dict[Filename, Tuple[MetaManifest, dict]]

    """
    # Sanity check: all `kinds` must be supported or we abort.
    if not set(kinds).issubset(SUPPORTED_KINDS):
        unsupported = set(kinds) - set(SUPPORTED_KINDS)
        logit.error(f"Cannot sync unsupported kinds: {unsupported}")
        return RetVal(None, True)

    # Avoid side effects.
    server_manifests = copy.deepcopy(server_manifests)

    # If the user did not specify any namespaces then we will operate on all
    # the namespaces that are currently in K8s.
    if namespaces is None:
        namespaces = {meta.namespace for meta in server_manifests}

    # Only retain server manifests with correct `kinds` and `namespaces`.
    server_manifests = {
        meta: v for meta, v in server_manifests.items()
        if meta.kind in kinds and meta.namespace in namespaces
    }

    # Add all local manifests outside the specified `kinds` and `namespaces`
    # to the server list. This will *not* propagate to the server in any way,
    # but allows us to make the rest of the function oblivious to the fact that
    # we only care about a subset of namespaces and resources by pretending
    # that local and server manifests are already in sync.
    for fname, manifests in local_manifests.items():
        for meta, manifest in manifests:
            if meta.kind in kinds and meta.namespace in namespaces:
                continue
            server_manifests[meta] = manifest
    del kinds, namespaces

    # Create map for MetaManifest -> (File, doc-idx). The doc-idx denotes the
    # index of the manifest inside the YAML files (it may contain multiple
    # manifests). We will need that information later to find out which
    # manifest in which file we need to update.
    meta_to_fname = {}
    for fname in local_manifests:
        for idx, (meta, _) in enumerate(local_manifests[fname]):
            meta_to_fname[meta] = (fname, idx)
            del meta
        del fname

    # Make a copy of the local manifests to avoid side effects for the caller.
    # Also put it into a default dict for convenience.
    out_add_mod = collections.defaultdict(list)
    out_add_mod.update(copy.deepcopy(local_manifests))
    del local_manifests

    # If the server's meta manifest exists locally then update the local one,
    # otherwise add it to f"_{namespace}.yaml".
    for meta, manifest in server_manifests.items():
        try:
            # Find the file that defined `meta` and its position inside that file.
            fname, idx = meta_to_fname[meta]
        except KeyError:
            # Put the resource into the correct "catch-all" file. However, we
            # must first determine the namespace, which differs depending on
            # whether the resource is a Namespace or other resource.
            if meta.kind == "Namespace":
                catch_all = f"_{meta.name}.yaml"
            else:
                catch_all = f"_{meta.namespace}.yaml"
            out_add_mod[catch_all].append((meta, manifest))
        else:
            # Update the correct YAML document in the correct file.
            out_add_mod[fname][idx] = (meta, manifest)

    # Iterate over all manifests in all files and drop the resources that do
    # not exist on the server. This will, in effect, delete those resources in
    # the local files if the caller chooser to save them.
    out_add_mod_del = {}
    for fname, manifests in out_add_mod.items():
        pruned = [(meta, man) for (meta, man) in manifests if meta in server_manifests]
        out_add_mod_del[fname] = pruned

    return RetVal(out_add_mod_del, False)


def unparse(file_manifests):
    """Convert the Python dict to a Yaml string for each file and return it.

    The output dict can be passed directly to `save_files` to write the files.

    Inputs:
        file_manifests: Dict[Filename:Tuple[MetaManifest, manifest]]
            Typically the output from eg `manio.sync`.

    Returns:
        Dict[Filename:YamlStr]: Yaml representation of all manifests.

    """
    out = {}
    for fname, manifests in file_manifests.items():
        # Verify that this file contains only supported resource kinds.
        kinds = {meta.kind for meta, _ in manifests}
        delta = kinds - set(SUPPORTED_KINDS)
        if len(delta) > 0:
            logit.error(f"Found unsupported KIND when writing <{fname}>: {delta}")
            return RetVal(None, True)

        # Group the manifests by their "kind", sort each group and compile a
        # new list of grouped and sorted manifests.
        man_sorted = []
        for kind in SUPPORTED_KINDS:
            man_sorted += sorted([_ for _ in manifests if _[0].kind == kind])
        assert len(man_sorted) == len(manifests)

        # Drop the MetaManifest, ie
        # Dict[Filename:Tuple[MetaManifest, manifest]] -> Dict[Filename:manifest]
        man_clean = [manifest for _, manifest in man_sorted]

        # Assign the grouped and sorted list of manifests to the output dict.
        out[fname] = man_clean
        del fname, manifests, man_sorted, man_clean

    # Ignore all files whose manifest list is empty.
    out = {k: v for k, v in out.items() if len(v) > 0}

    # Ensure that all dicts are pure Python dicts or there will be problems
    # with the YAML generation below.
    out_clean = {k: dotdict.undo(v) for k, v in out.items()}

    # Convert all manifest dicts into YAML strings.
    out = {}
    try:
        for fname, v in out_clean.items():
            out[fname] = yaml.safe_dump_all(v, default_flow_style=False)
    except yaml.YAMLError as err:
        logit.error(
            f"YAML error. Cannot create <{fname}>: {err.args[0]} <{str(err.args[1])}>"
        )
        return RetVal(None, True)

    # Return the Dict[Filename:YamlStr]
    return RetVal(out, False)


def save_files(base_path, file_data: dict):
    """Save all `file_data` under `base_path`.

    All paths in `file_data` are relative to `base_path`.

    Inputs:
        base_path: Path
        file_data: Dict[Filename, str]
            The file name (relative to `base_path`) and its content.

    Returns:
        None

    """
    # Python's `pathlib.Path` objects are simply nicer to work with...
    base_path = pathlib.Path(base_path)

    # Delete all YAML files under `base_path`. This avoids stale manifests.
    try:
        for fp in base_path.rglob("*.yaml"):
            logit.info(f"Removing stale <{fp}>")
            fp.unlink()
    except (IOError, PermissionError) as err:
        logit.error(f"{err}")
        return RetVal(None, True)

    # Iterate over the dict and write each file. Abort on error.
    for fname, yaml_str in file_data.items():
        # Skip the file if its content would be empty.
        if yaml_str == '':
            continue

        # Construct absolute file path.
        fname_abs = base_path / fname
        logit.debug(f"Creating path for <{fname}>")

        # Create the parent directories and write the file. Abort on error.
        logit.info(f"Saving YAML file <{fname_abs}>")
        try:
            fname_abs.parent.mkdir(parents=True, exist_ok=True)
            fname_abs.write_text(yaml_str)
        except (IOError, PermissionError) as err:
            logit.error(f"{err}")
            return RetVal(None, True)

    # Tell caller that all files were successfully written.
    return RetVal(None, False)


def load_files(base_path, fnames: tuple):
    """Load all `fnames` relative `base_path`.

    The elements of `fname` can have sub-paths, eg `foo/bar/file.txt` is valid
    and would ultimately open f"{base_path}/foo/bar/file.txt".

    Either returns the content of all files or returns with an error and no
    data. It will not return only a sub-set of the files.

    Inputs:
        base_path: Path
        fnames: Iterable[str|Path]
            The file names relative to `base_path`.

    Returns:
        Dict[Filename, str]: the file names (relative to `base_path`) and their
        content as a string.

    """
    # Python's `pathlib.Path` objects are simply nicer to work with...
    base_path = pathlib.Path(base_path)

    # Load each file and store its name and content in the `out` dictionary.
    out = {}
    for fname_rel in fnames:
        # Construct absolute file path.
        fname_abs = base_path / fname_rel
        logit.debug(f"Loading {fname_abs}")

        # Read the file. Abort on error.
        try:
            # The str() is necessary because `fname_rel` may be a `pathlib.Path`.
            out[str(fname_rel)] = fname_abs.read_text()
        except FileNotFoundError:
            logit.error(f"Could not find <{fname_abs}>")
            return RetVal(None, True)

    # Return the read files.
    return RetVal(out, False)


def load(folder):
    """Load all "*.yaml" files under `folder` (recursively).

    Ignores all files not ending in ".yaml".

    Returns no data in the case of an error.

    NOTE: this is merely a wrapper around the various low-level functions to
    load and parse the YAML files.

    Input:
        folder: str|Path
            Source folder.

    Returns:
        Dict[Filename, Tuple(MetaManifest, dict)]: parsed YAML files.

    """
    # Python's `pathlib.Path` objects are simply nicer to work with...
    folder = pathlib.Path(folder)

    # Compile the list of all YAML files in `folder` but only store their path
    # relative to `folder`.
    fnames = [_.relative_to(folder) for _ in folder.rglob("*.yaml")]

    # Load the files and abort on error.
    fdata_raw, err = load_files(folder, fnames)
    if err:
        return RetVal(None, True)

    # Return the YAML parsed manifests.
    return parse(fdata_raw)


def save(folder, manifests: dict):
    """Convert all `manifests` to YAML and save them.

    Returns no data in the case of an error.

    NOTE: this is merely a wrapper around the various low-level functions to
    create YAML string and save the files.

    Input:
        folder: str|Path
            Source folder.
        file_manifests: Dict[Filename, Tuple(MetaManifest, dict)]
            Names of files and their Python dicts to save as YAML.

    Returns:
        None

    """
    # Python's `pathlib.Path` objects are simply nicer to work with...
    folder = pathlib.Path(folder)

    # Convert the manifest to YAML strings. Abort on error.
    fdata_raw, err = unparse(manifests)
    if err:
        return RetVal(None, True)

    # Save the files to disk.
    return save_files(folder, fdata_raw)


def diff(config, local: dict, server: dict):
    """Return the human readable diff between the `local` and `server`.

    The diff shows the necessary changes to transition the `server` manifest
    into the state of the `local` manifest.

    Inputs:
        config: k8s.Config
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
    srv, err1 = strip(config, server)
    loc, err2 = strip(config, local)
    if err1 or err2:
        return RetVal(None, True)

    # Undo the DotDicts. This is a pre-caution because the YAML parser can
    # otherwise not dump the manifests.
    srv = dotdict.undo(srv)
    loc = dotdict.undo(loc)
    srv_lines = yaml.dump(srv, default_flow_style=False).splitlines()
    loc_lines = yaml.dump(loc, default_flow_style=False).splitlines()

    # Compute and return the lines of the diff.
    diff_lines = difflib.unified_diff(srv_lines, loc_lines, lineterm='')
    return RetVal(str.join("\n", diff_lines), False)


def strip(config, manifest):
    """Return stripped version of `manifest` with only the essential keys.

    The "essential" keys for each supported resource type are defined in the
    `schemas` module. In the context of `square`, essential keys are those that
    specify a resource (eg "kind" or "metadata.name") but not derivative
    information like "metadata.creationTimestamp" or "status".

    Inputs:
        config: k8s.Config
        manifest: dict

    Returns:
        dict: subset of `manifest`.

    """
    # Avoid side effects.
    manifest = copy.deepcopy(manifest)

    # Every manifest must specify its "apiVersion" and "kind".
    try:
        kind, version = manifest["kind"], manifest["apiVersion"]
    except KeyError as err:
        logit.error(f"Manifest is missing the <{err.args[0]}> key.")
        return RetVal(None, True)

    def _update(schema, manifest, out):
        """Recursively traverse the `schema` dict and add `manifest` keys into `out`.

        Incorporate the mandatory and optional keys.

        Raise `KeyError` if an invalid key was found.

        """
        # Iterate over every key/value pair of the schema and copy the
        # mandatory and optional keys. Raise an error if we find a key in
        # `manifest` that should not be there according to the schema.
        for k, v in schema.items():
            if v is True:
                # This key must exist in `manifest` and will be included.
                if k not in manifest:
                    logit.error(f"<{kind}> manifest must have a <{k}> key")
                    raise KeyError
                out[k] = manifest[k]
            elif v is False:
                # This key must not exist in `manifest` and will be excluded.
                if k in manifest:
                    logit.error(f"<{kind}> manifest must not have a <{k}> key")
                    raise KeyError
            elif v is None:
                # This key may exist in `manifest` and will be included in the
                # output if it does.
                if k in manifest:
                    out[k] = manifest[k]
            elif isinstance(v, dict):
                # The schema does not specify {True, False, None} but contains
                # another dict, which means we have to recurse into it.

                # Create a new dict level in the output dict. We will populate
                # it in when we recurse.
                out[k] = {}

                # Create a dummy dict in the input manifest if it lacks the
                # key. This is a corner case where the schema specifies a key
                # that contains only optional sub-keys. Since we do not know
                # yet if they will all be optional, we create an empty dict so
                # that the function can recurse.
                if k not in manifest:
                    manifest[k] = {}

                # Traverse all dicts down by one level and repeat the process.
                _update(schema[k], manifest[k], out[k])

                # If all keys in `schema[k]` were optional then it is possible
                # that `out[k]` will be empty. If so, delete it because we do
                # not want to keep empty dicts around.
                if out[k] == {}:
                    del out[k]
            else:
                logit.error(f"This is a bug: type(v) = <{type(v)}")
                raise KeyError

    # Create preliminary output manifest.
    stripped = {"apiVersion": version, "kind": kind}

    # Verify the schema for the current resource and K8s version exist.
    try:
        schema = schemas.RESOURCE_SCHEMA[config.version][manifest["kind"]]
    except KeyError:
        logit.error(
            f"Unknown K8s version (<{config.version}>) "
            "or resource kind: <{kind}>"
        )
        return RetVal(None, True)

    # Strip down the manifest to its essential parts and return it.
    try:
        _update(schema, manifest, stripped)
    except KeyError:
        return RetVal(None, True)
    else:
        return RetVal(dotdict.make(stripped), False)
