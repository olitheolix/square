import collections
import copy
import difflib
import logging
import pathlib
from typing import DefaultDict, Dict, Iterable, List, Optional, Tuple

import square.dotdict
import square.k8s
import square.schemas
import yaml
import yaml.scanner
from square.dtypes import (
    SUPPORTED_KINDS, Filepath, GroupBy, K8sConfig, LocalManifestLists,
    LocalManifests, MetaManifest, Selectors, ServerManifests,
)

# Convenience: global logger instance to avoid repetitive code.
logit = logging.getLogger("square")
DotDict = square.dotdict.DotDict


def make_meta(manifest: dict) -> MetaManifest:
    """Compile `MetaManifest` information from `manifest` and return it.

    Throw `KeyError` if manifest lacks essential fields like `apiVersion`,
    `kind`, etc because it cannot possibly be a valid K8s manifest then.

    """
    # Unpack the namespace. For Namespace resources, this will be the "name".
    if manifest["kind"] == "Namespace":
        ns = manifest['metadata']['name']
    else:
        # For non-Namespace manifests, the namespace may genuinely be None if
        # the resource applies globally, eg ClusterRole.
        ns = manifest['metadata'].get("namespace", None)

    # Return the populated MetaManifest.
    return MetaManifest(
        apiVersion=manifest['apiVersion'],
        kind=manifest['kind'],
        namespace=ns,
        name=manifest['metadata']['name']
    )


def select(manifest: dict, selectors: Selectors) -> bool:
    """Return `False` unless `manifest` satisfies _all_ `selectors`.

    Inputs:
        manifests: dict
        selectors: Selectors,

    Returns:
        bool: `True` iff the resource matches all selectors.

    """
    # "kinds" cannot be an empty list or `None`.
    if not selectors.kinds:
        logit.error(f"BUG: selector must specify a `kind`: {selectors}")
        return False

    # Unpack the resource's kind and labels.
    kind = manifest.get("kind", None)
    labels = manifest.get("metadata", {}).get("labels", {})
    name = manifest.get("metadata", {}).get("name", "")

    # Unpack the resource's namespace.
    ns = manifest.get("metadata", {}).get("namespace", None)

    # We need to pay special attention to `Namespace` resources since they are
    # not themselves namespaced.
    #
    # Furthermore, we *never* mess with `default-token-*` Secrets or the
    # `default` service account. K8s automatically creates them in every new
    # namespace. We can thus never "restore" them with Square because the plan
    # says to create them yet once Square created the namespace they also
    # already exist and can only be patched. As a result, Square will abort
    # unexpectedly because they already exist even though the plan said to
    # create them.
    if kind == "Namespace":
        ns = manifest.get("metadata", {}).get("name", None)
    elif kind == "Secret":
        if name.startswith("default-token-"):
            logit.info("Skipping `default-token` Secret")
            return False
    elif kind == "ServiceAccount":
        if name == "default":
            logit.info("Skipping `default` service account")
            return False
    else:
        pass

    # Proceed only if the resource kind is among the desired ones.
    if kind not in selectors.kinds:
        logit.debug(f"Kind {kind} does not match selector {selectors.kinds}")
        return False

    # Unless the namespace selector is None, the resource must match it.
    if selectors.namespaces is not None:
        if ns not in selectors.namespaces:
            logit.debug(f"Namespace {ns} does not match selector {selectors.namespaces}")
            return False

    # Convert the labels dictionary into a set of (key, value) tuples. We can
    # then use set logic to determine if the resource specifies the desired
    # labels or not.
    labels = {(k, v) for k, v in labels.items()}

    # Unless the label selector is None, the resource must match it.
    if selectors.labels is not None:
        if not selectors.labels.issubset(labels):
            logit.debug(f"Labels {labels} do not match selector {selectors.labels}")
            return False

    # If we get to here then the resource matches all selectors.
    return True


def unpack_list(manifest_list: dict,
                selectors: Selectors) -> Tuple[Optional[ServerManifests], bool]:
    """Unpack a K8s List item, eg `DeploymentList` or `NamespaceList`.

    Return a dictionary where each key uniquely identifies the resource via a
    `MetaManifest` tuple and the value is the actual JSON `manifest`.

    Input:
        manifest_list: dict
            K8s response from GET request for eg `deployments`.
        selectors: Selectors

    Returns:
        dict[MetaManifest:dict]

    """
    # Ensure the server manifests have the essential fields. If not then
    # something is seriously wrong.
    must_have = ("apiVersion", "kind", "items")
    missing = [key for key in must_have if key not in manifest_list]
    if len(missing) > 0:
        kind = manifest_list.get("kind", "UNKNOWN")
        logit.error(f"{kind} manifest is missing these keys: {missing}")
        return (None, True)
    del must_have, missing

    # Sanity check: resource kind must end in "List", eg "DeploymentList".
    kind = manifest_list["kind"]
    if not kind.endswith('List'):
        logit.error(f"Kind {kind} is not a list")
        return (None, True)

    # Strip of the "List".
    kind = kind[:-4]

    # Convenience.
    apiversion = manifest_list["apiVersion"]

    # Compile the manifests into a {MetaManifest: Manifest} dictionary. Skip
    # all the manifests that do not match the `selectors`.
    manifests = {}
    for manifest in manifest_list["items"]:
        # The "kind" key is missing from the manifest when K8s returns them in
        # a list. Here we manually add it again because it is part of every
        # properly formatted stand-alone manifest.
        manifest = copy.deepcopy(manifest)
        manifest["kind"] = kind
        manifest['apiVersion'] = apiversion
        if select(manifest, selectors):
            manifests[make_meta(manifest)] = manifest
    return (manifests, False)


def parse(
        file_yaml: Dict[Filepath, str],
        selectors: Selectors) -> Tuple[Optional[LocalManifestLists], bool]:
    """Parse all YAML strings from `file_yaml` into `LocalManifestLists`.

    Exclude all manifests that do not satisfy the `selectors`.

    Inputs:
        file_yaml: Dict[Filepath, str]
            Raw data as returned by `load_files`.
        selectors: Selectors
            Skip all manifests that do not match these `selectors`.

    Returns:
        LocalManifestLists: The YAML parsed manifests of each file.

    """
    # The output dict will have a list of tuples.
    out: LocalManifestLists = {}

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
            return (None, True)

        # Remove all empty manifests. This typically happens when the YAML
        # file ends with a "---" string.
        manifests = [_ for _ in manifests if _ is not None]

        # Retain only those manifests that satisfy the selectors.
        manifests = [_ for _ in manifests if select(_, selectors)]

        # Convert List[manifest] into List[(MetaManifest, manifest)].
        # Abort if `make_meta` throws a KeyError which happens if `file_yaml`
        # does not actually contain a Kubernetes manifest but some other
        # (valid) YAML.
        try:
            out[fname] = [(make_meta(_), _) for _ in manifests]
        except KeyError:
            logit.error(f"{file_yaml} does not look like a K8s manifest file.")
            return None, True

    # Drop all files without manifests.
    out = {k: v for k, v in out.items() if len(v) > 0}
    num_manifests = [len(_) for _ in out.values()]
    logit.debug(f"Parsed {sum(num_manifests)} manifests in {len(num_manifests)} files")

    # Return the YAML parsed manifests.
    return (out, False)


def unpack(manifests: LocalManifestLists) -> Tuple[Optional[ServerManifests], bool]:
    """Convert `manifests` to `ServerManifests` for internal processing.

    Returns `False` unless all resources in `manifests` are unique. For
    instance, returns False if two files define the same namespace or the same
    deployment.

    The primary use case is to convert the manifests we read from local files
    into the format Square uses internally for the server manifests as well.

    Inputs:
        manifests: LocalManifestLists

    Returns:
        ServerManifests: flattened version of `data`.

    """
    # Compile a dict that shows which meta manifest was defined in which file.
    # We will shortly use this information to determine if all resources were
    # defined exactly once across all files.
    all_meta: DefaultDict[MetaManifest, list] = collections.defaultdict(list)
    for fname in manifests:
        for meta, _ in manifests[fname]:
            all_meta[meta].append(fname)

    # Find out if all meta manifests were unique. If not, log the culprits and
    # return with an error.
    unique = True
    for meta, fnames in all_meta.items():
        if len(fnames) > 1:
            unique = False
            logit.error(
                f"Duplicate ({len(fnames)}x) manifest {meta}. "
                f"Defined in {str.join(', ', fnames)}"
            )
    if not unique:
        return (None, True)

    # Compile the input manifests into a new dict with the meta manifest as key.
    out = {k: v for fname in manifests for k, v in manifests[fname]}
    return (out, False)


def unparse(
        file_manifests: LocalManifestLists
) -> Tuple[Optional[Dict[Filepath, str]], bool]:
    """Convert the Python dict to a Yaml string for each file and return it.

    The output dict can be passed directly to `save_files` to write the files.

    Inputs:
        file_manifests: Dict[Filepath:Tuple[MetaManifest, manifest]]
            Typically the output from eg `manio.sync`.

    Returns:
        Dict[Filepath:YamlStr]: Yaml representation of all manifests.

    """
    out = {}
    for fname, manifests in file_manifests.items():
        # Verify that this file contains only supported resource kinds.
        kinds = {meta.kind for meta, _ in manifests}
        delta = kinds - set(SUPPORTED_KINDS)
        if len(delta) > 0:
            logit.error(f"Found unsupported KIND when writing <{fname}>: {delta}")
            return (None, True)

        # Group the manifests by their "kind", sort each group and compile a
        # new list of grouped and sorted manifests.
        man_sorted: List[dict] = []
        for kind in SUPPORTED_KINDS:
            man_sorted += sorted([_ for _ in manifests if _[0].kind == kind])
        assert len(man_sorted) == len(manifests)

        # Drop the MetaManifest, ie
        # Dict[Filepath:Tuple[MetaManifest, manifest]] -> Dict[Filepath:manifest]
        man_clean = [manifest for _, manifest in man_sorted]

        # Assign the grouped and sorted list of manifests to the output dict.
        out[fname] = man_clean
        del fname, manifests, man_sorted, man_clean

    # Ignore all files whose manifest list is empty.
    out_nonempty = {k: v for k, v in out.items() if len(v) > 0}
    del out

    # Ensure that all dicts are pure Python dicts or there will be problems
    # with the YAML generation below.
    out_clean = {k: square.dotdict.undo(v) for k, v in out_nonempty.items()}
    del out_nonempty

    # Convert all manifest dicts into YAML strings.
    out_final: Dict[Filepath, str] = {}
    try:
        for fname, v in out_clean.items():
            out_final[fname] = yaml.safe_dump_all(v, default_flow_style=False)
    except yaml.YAMLError as err:
        logit.error(
            f"YAML error. Cannot create <{fname}>: {err.args[0]} <{str(err.args[1])}>"
        )
        return (None, True)

    # Return the Dict[Filepath:YamlStr]
    return (out_final, False)


def sync(
        local_manifests: LocalManifestLists,
        server_manifests: ServerManifests,
        selectors: Selectors,
        groupby: GroupBy,
) -> Tuple[Optional[LocalManifestLists], bool]:
    """Update the local manifests with the server values and return the result.

    Inputs:
        local_manifests: Dict[Filepath, Tuple[MetaManifest, dict]]
        server_manifests: Dict[MetaManifest, dict]
        selectors: Selectors
            Only operate on resources that match the selectors.
        groupby: GroupBy
            Specify relationship between new manifests and file names.

    Returns:
        Dict[Filepath, Tuple[MetaManifest, dict]]

    """
    # Sanity check: all `kinds` must be supported or we abort.
    if not set(selectors.kinds).issubset(SUPPORTED_KINDS):
        unsupported = set(selectors.kinds) - set(SUPPORTED_KINDS)
        logit.error(f"Cannot sync unsupported kinds: {unsupported}")
        return (None, True)

    # Avoid side effects.
    server_manifests = copy.deepcopy(server_manifests)

    # Only retain server manifests with correct `kinds` and `namespaces`.
    server_manifests = {
        meta: manifest for meta, manifest in server_manifests.items()
        if select(manifest, selectors)
    }

    # Add all local manifests outside the specified `kinds` and `namespaces`
    # to the server list. This will *not* propagate to the server in any way,
    # but allows us to make the rest of the function oblivious to the fact that
    # we only care about a subset of namespaces and resources by pretending
    # that local and server manifests are already in sync.
    for fname, manifests in local_manifests.items():
        for meta, manifest in manifests:
            if select(manifest, selectors):
                continue
            server_manifests[meta] = manifest

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
    out_add_mod: DefaultDict[Filepath, List[Tuple[MetaManifest, dict]]]
    out_add_mod = collections.defaultdict(list)
    out_add_mod.update(copy.deepcopy(local_manifests))  # type: ignore
    del local_manifests

    # If the server's meta manifest exists locally then update the local one,
    # otherwise add it to the catchall YAML file.
    for meta, manifest in server_manifests.items():
        try:
            # Find the file that defined `meta` and its position inside that file.
            fname, idx = meta_to_fname[meta]
        except KeyError:
            fname, err = filename_for_manifest(meta, manifest, groupby)
            if err:
                return (None, True)
            out_add_mod[fname].append((meta, manifest))
        else:
            # Update the correct YAML document in the correct file.
            out_add_mod[fname][idx] = (meta, manifest)

    # Iterate over all manifests in all files and drop the resources that do
    # not exist on the server. This will, in effect, delete those resources in
    # the local files if the caller chose to save them.
    out_add_mod_del: LocalManifestLists = {}
    for fname, manifests in out_add_mod.items():
        pruned = [(meta, man) for (meta, man) in manifests if meta in server_manifests]
        out_add_mod_del[fname] = pruned

    return (out_add_mod_del, False)


def filename_for_manifest(
        meta: MetaManifest, manifest: dict,
        grouping: GroupBy) -> Tuple[Filepath, bool]:
    """Return the file for the manifest based on `groupby`.

    Inputs:
        meta: MetaManifest
        manifest: dict
        groupby: GroupBy

    Output:
        Filepath

    """
    # --- Sanity checks ---
    if not set(grouping.order).issubset({"ns", "kind", "label"}):
        logit.error(f"Invalid resource ordering: {grouping.order}")
        return Filepath(), True

    if "label" in grouping.order:
        if len(grouping.label) == 0:
            logit.error("Must specify a non-empty label when grouping by it")
            return Filepath(), True

    # Convenience: reliably extract a label dictionary even when the original
    # manifest has none.
    labels = manifest.get("metadata", {}).get("labels", {})

    # Helper LookUpTable that contains the values for all those groups the
    # "--groupby" command line option accepts. We will use this LUT below to
    # assemble the full manifest path.
    lut = {
        # Get the namespace. Use "_global_" if the resource's namespace is
        # None, which it will be for global resources like ClusterRole and
        # ClusterRoleBinding.
        "ns": meta.namespace or "_global_",
        "kind": meta.kind.lower(),
        # Try to find the user specified label. If the current resource lacks
        # that label then put it into the catchall file.
        "label": labels.get(grouping.label, "_other"),
    }

    # Concatenate the components according to `grouping.order` to produce the
    # full file name. This order is what the user can specify via the
    # "--groupby" option on the command line.
    path_constituents = [lut[_] for _ in grouping.order]
    path = str.join("/", path_constituents)

    # Default to the catch-all `_other.yaml` resource if the order did not
    # produce a file name. This typically happens when `grouping.order = []`.
    path = "_other.yaml" if path == "" else f"{path}.yaml"
    return Filepath(path), False


def diff(
        config: K8sConfig,
        local: LocalManifests,
        server: ServerManifests) -> Tuple[Optional[str], bool]:
    """Return the human readable diff between the `local` and `server`.

    The diff shows the necessary changes to transition the `server` manifest
    into the state of the `local` manifest.

    Inputs:
        config: K8sConfig
        local: dict
            Local manifest.
        server: dict
            Local manifest.

    Returns:
        str: human readable diff string as the Unix `diff` utility would
        produce it.

    """
    # Clean up the input manifests because we do not want to diff, for instance,
    # the `status` fields.
    srv, err1 = strip(config, server)
    loc, err2 = strip(config, local)
    if err1 or err2:
        return (None, True)

    # Undo the DotDicts. This is a precaution because the YAML parser can
    # otherwise not dump the manifests.
    srv = square.dotdict.undo(srv)
    loc = square.dotdict.undo(loc)
    srv_lines = yaml.dump(srv, default_flow_style=False).splitlines()
    loc_lines = yaml.dump(loc, default_flow_style=False).splitlines()

    # Compute and return the lines of the diff.
    diff_lines = difflib.unified_diff(srv_lines, loc_lines, lineterm='')
    return (str.join("\n", diff_lines), False)


def strip(config: K8sConfig, manifest: dict) -> Tuple[Optional[DotDict], bool]:
    """Return stripped version of `manifest` with only the essential keys.

    The "essential" keys for each supported resource type are defined in the
    `schemas` module. In the context of `square`, essential keys are those that
    specify a resource (eg "kind" or "metadata.name") but not derivative
    information like "metadata.creationTimestamp" or "status".

    Inputs:
        config: K8sConfig
        manifest: dict

    Returns:
        dict: subset of `manifest`.

    """
    assert config.version is not None

    # Avoid side effects.
    manifest = copy.deepcopy(manifest)

    # Every manifest must specify its "apiVersion" and "kind".
    try:
        kind = manifest["kind"]
        version = manifest["apiVersion"]
    except KeyError as err:
        logit.error(f"Manifest is missing the <{err.args[0]}> key.")
        return (None, True)

    # Unpack the name and namespace to produce a convenient log message.
    # NOTE: we assume here that manifests may not have either.
    name = manifest.get("metadata", {}).get("name", "unknown")
    namespace = manifest.get("metadata", {}).get("namespace", "unknown")
    man_id = f"{kind.upper()}: {namespace}/{name}"
    del name, namespace

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
                    logit.error(f"{man_id} must have a <{k}> key")
                    raise KeyError
                out[k] = manifest[k]
            elif v is False:
                # This key must not exist in `manifest` and will be excluded.
                if k in manifest:
                    logit.error(f"{man_id} must not have a <{k}> key")
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
        schema = square.schemas.RESOURCE_SCHEMA[config.version][manifest["kind"]]
    except KeyError:
        logit.error(
            f"Unknown K8s version (<{config.version}>) "
            "or resource kind: <{kind}>"
        )
        return (None, True)

    # Strip down the manifest to its essential parts and return it.
    try:
        _update(schema, manifest, stripped)
    except KeyError:
        return (None, True)
    else:
        return (square.dotdict.make(stripped), False)


def save_files(folder: Filepath, file_data: Dict[Filepath, str]) -> Tuple[None, bool]:
    """Save all `file_data` under `folder`.

    All paths in `file_data` are relative to `folder`.

    Inputs:
        folder: Filepath
        file_data: Dict[Filepath, str]
            The file name (relative to `folder`) and its content.

    Returns:
        None

    """
    # Python's `pathlib.Path` objects are simply nicer to work with...
    folder = pathlib.Path(folder)

    # Delete all YAML files under `folder`. This avoids stale manifests.
    try:
        for fp in folder.rglob("*.yaml"):
            logit.info(f"Removing stale <{fp}>")
            fp.unlink()
    except (IOError, PermissionError) as err:
        logit.error(f"{err}")
        return (None, True)

    # Iterate over the dict and write each file. Abort on error.
    for fname, yaml_str in file_data.items():
        # Skip the file if its content would be empty.
        if yaml_str == '':
            continue

        # Construct absolute file path.
        fname_abs = folder / fname
        logit.debug(f"Creating path for <{fname}>")

        # Create the parent directories and write the file. Abort on error.
        logit.info(f"Saving YAML file <{fname_abs}>")
        try:
            fname_abs.parent.mkdir(parents=True, exist_ok=True)
            fname_abs.write_text(yaml_str)
        except (IOError, PermissionError) as err:
            logit.error(f"{err}")
            return (None, True)

    # Tell caller that all files were successfully written.
    return (None, False)


def load_files(
        folder: Filepath,
        fnames: Iterable[Filepath]) -> Tuple[Optional[Dict[Filepath, str]], bool]:
    """Load all `fnames` in `folder` and return their content.

    The elements of `fname` can have sub-paths, eg `foo/bar/file.txt` is valid
    and would ultimately open f"{folder}/foo/bar/file.txt".

    Either returns the content of all files or returns with an error and no
    data. It will not return only a sub-set of the files.

    Inputs:
        folder: Path
        fnames: Iterable[str|Path]
            The file names relative to `folder`.

    Returns:
        Dict[Filepath, str]: the file names (relative to `folder`) and their
        content as a string.

    """
    # Python's `pathlib.Path` objects are simply nicer to work with...
    folder = pathlib.Path(folder)

    # Load each file and store its name and content in the `out` dictionary.
    out: Dict[Filepath, str] = {}
    for fname_rel in fnames:
        # Construct absolute file path.
        fname_abs = folder / fname_rel
        logit.debug(f"Loading {fname_abs}")

        # Read the file. Abort on error.
        try:
            # The str() is necessary because `fname_rel` may be a `pathlib.Path`.
            out[fname_rel] = fname_abs.read_text()
        except FileNotFoundError:
            logit.error(f"Could not find <{fname_abs}>")
            return (None, True)

    # Return the read files.
    return (out, False)


def load(folder: Filepath, selectors: Selectors) -> Tuple[
        Optional[ServerManifests], Optional[LocalManifestLists], bool]:
    """Recursively load all "*.yaml" files under `folder`.

    Ignores all files not ending in ".yaml". Also removes all manifests that do
    not match the `selectors`.

    Returns no data in the case of an error.

    NOTE: this is merely a wrapper around the various low-level functions to
    load and parse the YAML files.

    Input:
        folder: Filepath
            Source folder.
        selectors: Selectors

    Returns:
        (local manifest without file info, local manifests with file info)

    """
    # Python's `pathlib.Path` objects are simply nicer to work with...
    folder = pathlib.Path(folder)

    # Compile the list of all YAML files in `folder` but only store their path
    # relative to `folder`.
    fnames = [_.relative_to(folder) for _ in folder.rglob("*.yaml")]

    try:
        # Load the files and abort on error.
        fdata_raw, err = load_files(folder, fnames)
        assert not err and fdata_raw is not None

        # Return the YAML parsed manifests.
        man_files, err = parse(fdata_raw, selectors)
        assert not err and man_files is not None

        # Remove the Filepath dimension.
        man_meta, err = unpack(man_files)
        assert not err and man_meta is not None
    except AssertionError:
        return (None, None, True)

    # Return the file based manifests and unpacked manifests.
    return (man_meta, man_files, False)


def save(folder: Filepath, manifests: LocalManifestLists) -> Tuple[None, bool]:
    """Convert all `manifests` to YAML and save them.

    Returns no data in the case of an error.

    NOTE: this is merely a wrapper around the various low-level functions to
    create YAML string and save the files.

    Input:
        folder: Filepath
            Source folder.
        file_manifests: Dict[Filepath, Tuple(MetaManifest, dict)]
            Names of files and their Python dicts to save as YAML.

    Returns:
        None

    """
    # Convert the manifest to YAML strings. Abort on error.
    fdata_raw, err = unparse(manifests)
    if err or fdata_raw is None:
        return (None, True)

    # Save the files to disk.
    return save_files(folder, fdata_raw)


def download(
        config: K8sConfig,
        client,
        selectors: Selectors,
) -> Tuple[Optional[ServerManifests], bool]:
    """Download and return the resources that match `selectors`.

    Set `selectors.namespace` to `None` to download the resources from all
    Kubernetes namespaces.

    Either returns all the data or an error; never returns partial results.

    Inputs:
        config: K8sConfig
        client: `requests` session with correct K8s certificates.
        selectors: Selectors

    Returns:
        Dict[MetaManifest, dict]: the K8s manifests from K8s.

    """
    # Output.
    server_manifests = {}

    # Ensure `namespaces` is always a list to avoid special casing below.
    all_namespaces: Iterable[Optional[str]]
    if selectors.namespaces is None:
        all_namespaces = [None]
    else:
        all_namespaces = selectors.namespaces

    # Download each resource type. Abort at the first error and return nothing.
    for namespace in all_namespaces:
        for kind in selectors.kinds:
            try:
                # Get the HTTP URL for the resource request.
                url, err = square.k8s.urlpath(config, kind, namespace)
                assert not err and url is not None

                # Make HTTP request.
                manifest_list, err = square.k8s.get(client, url)
                assert not err and manifest_list is not None

                # Parse the K8s List (eg DeploymentList, NamespaceList, ...) into a
                # Dict[MetaManifest, dict] dictionary.
                manifests, err = unpack_list(manifest_list, selectors)
                assert not err and manifests is not None

                # Drop all manifest fields except "apiVersion", "metadata" and "spec".
                ret = {k: strip(config, man) for k, man in manifests.items()}

                # Ensure `strip` worked for every manifest.
                err = any((v[1] for v in ret.values()))
                assert not err

                # Unpack the stripped manifests from the `strip` response.
                # The "if v[0] is not None" is to satisfy MyPy - we already
                # know they are not None or otherwise the previous assert would
                # have failed.
                manifests = {k: v[0] for k, v in ret.items() if v[0] is not None}
            except AssertionError:
                # Return nothing, even if we had downloaded other kinds already.
                return (None, True)
            else:
                # Copy the manifests into the output dictionary.
                server_manifests.update(manifests)
    return (server_manifests, False)
