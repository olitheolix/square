import collections
import copy
import difflib
import logging
import pathlib
from typing import (
    Collection, DefaultDict, Dict, Iterable, List, Optional, Tuple,
)

import square.cfgfile
import square.dotdict
import square.k8s
import yaml
import yaml.scanner
from square.dtypes import (
    Config, Filepath, GroupBy, K8sConfig, K8sResource, LocalManifestLists,
    MetaManifest, Selectors, ServerManifests,
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
        ns = None
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

    # Split all labels into tuples: "foo=bar" -> ("foo", "bar").
    # Sanity check: hard abort if the labels do not split.
    label_selectors = {tuple(_.split("=")) for _ in selectors.labels}
    assert all([len(_) == 2 for _ in label_selectors])

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
    # already exist and can only be patched. As a result, Square would abort
    # unexpectedly.
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

    # Include the resource if it is a) namespaced b) we have namespace
    # selectors and c) the resource matches one of them.
    if ns and selectors.namespaces and ns not in selectors.namespaces:
        logit.debug(f"Namespace {ns} does not match selector {selectors.namespaces}")
        return False

    # Convert the labels dictionary into a set of (key, value) tuples. We can
    # then use set logic to determine if the resource specifies the desired
    # labels or not.
    labels = {(k, v) for k, v in labels.items()}

    # The resource must match the label selectors unless we have no such selectors.
    if label_selectors:
        if not label_selectors.issubset(labels):
            logit.debug(f"Labels {labels} do not match selector {selectors.labels}")
            return False

    # If we get to here then the resource matches all selectors.
    return True


def unpack_list(manifest_list: dict,
                selectors: Selectors) -> Tuple[ServerManifests, bool]:
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
        return ({}, True)
    del must_have, missing

    # Sanity check: resource kind must end in "List", eg "DeploymentList".
    kind = manifest_list["kind"]
    if not kind.endswith('List'):
        logit.error(f"Kind {kind} is not a list")
        return ({}, True)

    # Strip off the "List" suffix from eg "DeploymentList".
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
        selectors: Selectors) -> Tuple[LocalManifestLists, bool]:
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
        except (yaml.parser.ParserError, yaml.scanner.ScannerError) as err:
            logit.error(
                f"Cannot YAML parse <{fname}>"
                f" - {err.problem} - Line {err.problem_mark.line}"
            )
            return ({}, True)

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
            return {}, True

    # Drop all files without manifests.
    out = {k: v for k, v in out.items() if len(v) > 0}
    num_manifests = [len(_) for _ in out.values()]
    logit.debug(f"Parsed {sum(num_manifests)} manifests in {len(num_manifests)} files")

    # Return the YAML parsed manifests.
    return (out, False)


def unpack(manifests: LocalManifestLists) -> Tuple[ServerManifests, bool]:
    """Convert `manifests` to `ServerManifests` for internal processing.

    Returns `False` unless all resources in `manifests` are unique. For
    instance, returns False if two files define the same namespace or the same
    deployment.

    The primary use case is to convert the manifests we read from local files
    into the format Square uses internally for the server manifests as well.

    Inputs:
        manifests: LocalManifestLists

    Returns:
        ServerManifests: flattened version of `manifests`.

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
            tmp = [str(_) for _ in fnames]
            logit.error(
                f"Duplicate ({len(tmp)}x) manifest {meta}. "
                f"Defined in {str.join(', ', tmp)}"
            )
    if not unique:
        return ({}, True)

    # Compile the input manifests into a new dict with the meta manifest as key.
    out = {k: v for fname in manifests for k, v in manifests[fname]}
    return (out, False)


def sync(local_manifests: LocalManifestLists,
         server_manifests: ServerManifests,
         selectors: Selectors,
         groupby: GroupBy) -> Tuple[LocalManifestLists, bool]:
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
                return ({}, True)
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
        # Get the namespace. Use "_global_" for non-namespaced resources.
        # The only exception are `Namespaces` themselves because it is neater
        # to save their manifest in the relevant namespace folder, together
        # with all the other resources that are in that namespace.
        "ns": (meta.name if meta.kind == "Namespace" else None) or meta.namespace or "_global_",  # noqa
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


def diff(config: Config,
         k8sconfig: K8sConfig,
         local: dict,
         server: dict) -> Tuple[str, bool]:
    """Return the human readable diff between the `local` and `server` manifest.

    The diff shows the necessary changes to transition the `server` manifest
    into the state of the `local` manifest.

    Inputs:
        config: Square configuration.
        k8sconfig: K8sConfig
        local: dict
            Local manifest.
        server: dict
            Local manifest.

    Returns:
        str: human readable diff string as the Unix `diff` utility would
        produce it.

    """
    # Precaution: undo the DotDicts to ensure the YAML parse will accept them.
    srv = square.dotdict.undo(server)
    loc = square.dotdict.undo(local)
    srv_lines = yaml.dump(srv, default_flow_style=False).splitlines()
    loc_lines = yaml.dump(loc, default_flow_style=False).splitlines()

    # Compute and return the lines of the diff.
    diff_lines = difflib.unified_diff(srv_lines, loc_lines, lineterm='')
    return (str.join("\n", diff_lines), False)


def strip(
    k8sconfig: K8sConfig,
    manifest: dict,
    manifest_filters: Dict[str, list],
) -> Tuple[DotDict, dict, bool]:
    """Strip `manifest` according to the filters in `square.cfgfile`.

    Inputs:
        k8sconfig: K8sConfig
        manifest: dict
        manifest_filters: Dict[str, list]
            See tests for examples

    Returns:
        dict, dict: (stripped manifest, removed keys).

    """
    # Convenience: default return value if an error occurs.
    ret_err: Tuple[DotDict, dict, bool] = (square.dotdict.make({}), {}, True)

    # Parse the manifest.
    try:
        meta = make_meta(manifest)
        resource, err = square.k8s.resource(k8sconfig, meta)
        assert not err
    except KeyError as e:
        logit.error(f"Manifest is missing the <{e.args[0]}> key.")
        return ret_err
    except AssertionError:
        return ret_err

    def _update(filters: list, manifest: dict):
        """Recursively traverse the `manifest` and prune it according to `filters`.

        Returns dict with the excluded keys.

        Raise `KeyError` if an invalid key was found.

        """
        # Split the list of strings and dicts into a dedicated set of string
        # and dedicated list of dicts.
        # Example: ["foo", "bar", {"a": "b", "c": "d"}] will become
        #   {"foo", "bar"} and {"a": "b", "c", "d"}.
        filter_str = {_ for _ in filters if isinstance(_, str)}
        filter_map = [_ for _ in filters if isinstance(_, dict)]
        filter_map = {k: v for d in filter_map for k, v in d.items()}

        # Iterate over the manifest. Prune all keys that match the `filters`
        # and record them in `removed`.
        removed = {}
        for k, v in list(manifest.items()):
            if k in filter_str:
                # Remove the entire key (and all sub-fields if present).
                # NOTE: it does not matter if the key also exists in
                # `filter_map` - we remove the entire key.
                logit.debug(f"Remove <{k}>")
                removed[k] = manifest.pop(k)
            elif isinstance(v, list) and k in filter_map:
                # Recursively filter each list element.
                tmp = [_update(filter_map[k], _) for _ in v]
                removed[k] = [_ for _ in tmp if _]

                # Do not leave empty elements in the list.
                manifest[k] = [_ for _ in v if _]
            elif isinstance(v, dict) and k in filter_map:
                # Recursively filter each dictionary element.
                logit.debug(f"Dive into <{k}>")
                removed[k] = _update(filter_map[k], manifest[k])
            else:
                logit.debug(f"Skip <{k}>")

            # Remove the key from the manifest altogether if it has become empty.
            if not manifest.get(k, "non-empty"):
                del manifest[k]

        # Remove all empty sub-dictionaries from `removed`.
        return {k: v for k, v in removed.items() if v != {}}

    # Look up the filters for the current resource in the following order:
    # 1) Supplied `filters`
    # 2) Square default filters for this resource `kind`.
    # 3) Square default filters that apply to all resource kinds.
    # Pick the first one that matches.
    kind = manifest["kind"]
    filters = manifest_filters.get(
        kind,
        square.DEFAULT_CONFIG.filters.get(
            kind,
            square.DEFAULT_CONFIG.filters["_common_"]
        )
    )
    if not square.cfgfile.valid(filters):
        return ret_err

    # Remove the keys from the `manifest` according to `filters`.
    manifest = copy.deepcopy(manifest)
    removed = _update(filters, manifest)
    return (square.dotdict.make(manifest), removed, False)


def align_serviceaccount(
        local_manifests: ServerManifests,
        server_manifests: ServerManifests) -> Tuple[ServerManifests, bool]:
    """Insert the token secret from `server_manifest` into `local_manifest`.

    Every ServiceAccount (SA) has a "secrets" section that K8s automatically
    populates when it creates the SA. The name contains a random hash, eg
    "default-token-somerandomhash" for the default service account in every
    namespace.

    This makes it difficult to manage service accounts with Square because the
    token is not known in advance. One would have to

        square apply; square plan; square get serviceaccount

    to sync this, and even that is not portable because the token will be
    different on a new cluster.

    To avoid this problem, this function will read the token secret that K8s
    added (contained in `server_manifest`) and insert it into the
    `local_manifest`. This will ensure that Square creates a plan that
    leaves the token secret alone.

    Inputs:
        local_manifests: manifests from local files that the plan will use.
        server_manifests: manifests from K8s

    Returns:
        Copy of `local_manifests` where all ServiceAccount token secrets match
        those of the server.

    """
    ReturnType = Tuple[Optional[str], List[Dict[str, str]], bool]

    def _get_token(meta: MetaManifest, manifests: ServerManifests) -> ReturnType:
        """Return token secret from `manifest` as well as all other other secrets.

        Example input manifest:
            {
                'apiVersion': v1,
                'kind': ServiceAccount,
                ...
                'secrets': [
                    {'name': 'some-secret'},
                    {'name': 'demoapp-token-abcde'},
                    {'name': 'other-secret'},
                ]
            }

        The output for this would be:
        (
            'demoapp-token-abcde',
            [{'name': 'some-secret'}, {'name': 'other-secret'}],
            False,
        )

        """
        # Do nothing if the ServiceAccount has no "secrets" - should be impossible.
        try:
            secrets_dict = manifests[meta]["secrets"]
        except KeyError:
            return (None, [], False)

        # Find the ServiceAccount token name.
        token_prefix = f"{meta.name}-token-"
        secrets = [_["name"] for _ in secrets_dict]
        token = [_ for _ in secrets if _.startswith(token_prefix)]

        if len(token) == 0:
            # No token - return the original secrets.
            return (None, secrets_dict, False)
        elif len(token) == 1:
            # Expected case: return the token as well as the remaining secrets.
            secrets = [{"name": _} for _ in secrets if _ != token[0]]
            return (token[0], secrets, False)
        else:
            # Unexpected.
            all_secrets = str.join(", ", list(sorted(token)))
            logit.warning(
                f"ServiceAccount <{meta.namespace}/{meta.name}>: "
                f"found multiple token secrets in: `{all_secrets}`"
            )
            return (None, [], True)

    # Avoid side effects.
    local_manifests = copy.deepcopy(local_manifests)

    # Find all ServiceAccount manifests that exist locally and on the cluster.
    local_meta = {k for k in local_manifests if k.kind == "ServiceAccount"}
    server_meta = set(server_manifests.keys()).intersection(local_meta)

    # Iterate over all ServiceAccount manifests and insert the secret token
    # from the cluster into the local manifest.
    for meta in server_meta:
        # Find the service account token in the local/cluster manifest.
        loc_token, loc_secrets, err1 = _get_token(meta, local_manifests)
        srv_token, srv_secrets, err2 = _get_token(meta, server_manifests)

        # Ignore the manifest if there was an error. Typically this means the
        # local or cluster manifest defined multiple service account secrets.
        # If that happens then something is probably seriously wrong with the
        # cluster.
        if err1 or err2:
            continue

        # Server has no token - something is probably wrong with your cluster.
        if srv_token is None:
            logit.warning(
                f"ServiceAccount {meta.namespace}/{meta.name} has no token secret"
            )
            continue

        # This is the expected case: the local manifest does not specify
        # the token but on the cluster it exists. In that case, add the
        # token here.
        if srv_token and not loc_token:
            loc_secrets.append({"name": srv_token})
            local_manifests[meta]["secrets"] = loc_secrets

    return (local_manifests, False)


def save_files(folder: Filepath, file_data: Dict[Filepath, str]) -> bool:
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
        return True

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
            return True

    # Tell caller that all files were successfully written.
    return False


def load_files(
        folder: Filepath,
        fnames: Iterable[Filepath]) -> Tuple[Dict[Filepath, str], bool]:
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
            return ({}, True)

    # Return the read files.
    return (out, False)


def load(folder: Filepath, selectors: Selectors) -> Tuple[
        ServerManifests, LocalManifestLists, bool]:
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
    fnames = [(_.relative_to(folder), _.name) for _ in folder.rglob("*.yaml")]
    fnames = [path for path, name in fnames if not name.startswith(".")]

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
        return ({}, {}, True)

    # Return the file based manifests and unpacked manifests.
    return (man_meta, man_files, False)


def sort_manifests(
        file_manifests: LocalManifestLists,
        priority: Collection[str]
) -> Tuple[Dict[Filepath, Collection[MetaManifest]], bool]:
    """Sort the manifests in each `file_manifests` by their `priority`.

    The returned data contains only the manifests without the `MetaData`. The
    idea is to pass that data directly to `save_files`.

    Inputs:
        file_manifests: the manifests that should go into each file.
        priority: Collection[str]
            Sort the manifest in this order, or alphabetically at the end if
            not in the list.

    """
    # Sort the manifests in each file.
    out: Dict[Filepath, Collection[MetaManifest]] = {}
    for fname, manifests in file_manifests.items():
        # Group the manifests by their "kind" in order of `priority` and sort
        # each group alphabetically.
        man_sorted: List[dict] = []
        for kind in priority:
            # Partition the manifest list into the current `kind` and the rest.
            tmp = [_ for _ in manifests if _[0].kind == kind]
            manifests = [_ for _ in manifests if _[0].kind != kind]

            # Append the manifests ordered by their MetaManifest.
            man_sorted += sorted(tmp, key=lambda _: _[0])

        # Group the remaining manifests by their "kind" and sort each group
        # alphabetically.
        remaining_kinds = {_[0].kind for _ in manifests}
        for kind in sorted(remaining_kinds):
            # Partition the manifest list into the current `kind` and the rest.
            tmp = [_ for _ in manifests if _[0].kind == kind]
            manifests = [_ for _ in manifests if _[0].kind != kind]

            # Append the manifests ordered by their MetaManifest.
            man_sorted += sorted(tmp, key=lambda _: _[0])

        # sanity check: we must have used up all manifests.
        assert len(manifests) == 0

        # Drop the MetaManifest, ie
        # Dict[Filepath:Tuple[MetaManifest, manifest]] -> Dict[Filepath:manifest]
        man_clean = [manifest for _, manifest in man_sorted]

        # Assign the grouped and sorted list of manifests to the output dict.
        out[fname] = man_clean

    return out, False


def save(folder: Filepath, manifests: LocalManifestLists,
         priority: Collection[str]) -> bool:
    """Saves all `manifests` as YAMLs in `folder`.

    Input:
        folder: Filepath
            Source folder.
        file_manifests: Dict[Filepath, Tuple(MetaManifest, dict)]
            Names of files and their Python dicts to save as YAML.
        priority: Collection[str]
            Sort the manifest in this order, or alphabetically at the end if
            not in the list.

    Returns:
        None

    """
    # Sort the manifest in each file by priority. Ignore the error flag because
    # `sort_manifests` always succeeds.
    out, _ = sort_manifests(manifests, priority)

    # Ignore all files without manifests, ie empty files.
    out_nonempty = {k: v for k, v in out.items() if len(v) > 0}
    del out

    # Ensure we have no `DotDicts` left. This will avoid problems with the YAML
    # serialisation below.
    out_clean = {k: square.dotdict.undo(v) for k, v in out_nonempty.items()}
    del out_nonempty

    # Ignore all hidden files.
    out_clean = {k: v for k, v in out_clean.items() if not k.name.startswith(".")}

    # Convert all manifest dicts into YAML strings.
    out_final: Dict[Filepath, str] = {}
    try:
        for fname, v in out_clean.items():
            out_final[fname] = yaml.safe_dump_all(v, default_flow_style=False)
    except yaml.YAMLError as e:
        logit.error(
            f"YAML error. Cannot create <{fname}>: {e.args[0]} <{str(e.args[1])}>"
        )
        return True

    # Save the files to disk.
    return save_files(folder, out_final)


def download(config: Config, k8sconfig: K8sConfig) -> Tuple[ServerManifests, bool]:
    """Download and return the resources that match `selectors`.

    Set `selectors.namespace` to `None` to download the resources from all
    Kubernetes namespaces.

    Either returns all the data or an error; never returns partial results.

    Inputs:
        config: Square configuration.
        k8sconfig: K8sConfig

    Returns:
        Dict[MetaManifest, dict]: the K8s manifests from K8s.

    """
    # Output.
    server_manifests = {}

    # Ensure `namespaces` is always a list to avoid special casing below.
    all_namespaces: Iterable[Optional[str]]
    if not config.selectors.namespaces:
        all_namespaces = [None]
    else:
        all_namespaces = config.selectors.namespaces

    # Download each resource type. Abort at the first error and return nothing.
    for namespace in all_namespaces:
        for kind in sorted(config.selectors.kinds):
            # Get the K8s URL for the current resource kind. Ignore this
            # resource if K8s does not know about it. The reason for that could
            # be a typo or that it is a Custom Resource that does not (yet) exist.
            resource, err = square.k8s.resource(k8sconfig, MetaManifest("", kind, namespace, ""))  # noqa
            if err:
                logit.warning(f"Skipping unknown resource <{kind}>")
                continue

            try:
                # Download the resource manifests for the current `kind` from K8s.
                manifest_list, err = square.k8s.get(k8sconfig.client, resource.url)
                assert not err and manifest_list is not None

                # Parse the K8s List (eg DeploymentList, NamespaceList, ...) into a
                # Dict[MetaManifest, dict] dictionary.
                manifests, err = unpack_list(manifest_list, config.selectors)
                assert not err and manifests is not None

                # Strip off the fields defined in `config.filters`.
                ret = {k: strip(k8sconfig, man, config.filters)
                       for k, man in manifests.items()}

                # Ensure `strip` worked for every manifest.
                err = any((v[2] for v in ret.values()))
                assert not err

                # Unpack the stripped manifests from the `strip` response. The
                # "if v[0] is not None" statement exists to satisfy MyPy - we
                # already know they are not None or otherwise the previous
                # assert would have failed.
                manifests = {k: v[0] for k, v in ret.items() if v[0] is not None}
            except AssertionError:
                # Return nothing, even if we had downloaded other kinds already.
                return ({}, True)
            else:
                # Copy the manifests into the output dictionary.
                server_manifests.update(manifests)
    return (server_manifests, False)


def download_single(k8sconfig: K8sConfig,
                    resource: K8sResource) -> Tuple[MetaManifest, dict, bool]:
    """Similar to `download(...)` but only for a single Kubernetes `resource`.

    Inputs:
        k8sconfig: K8sConfig
        resource: K8sResource

    Returns:
        MetaManifest, manifest: the K8s (meta)manifest.

    """
    try:
        # Download the resource.
        manifest, err = square.k8s.get(k8sconfig.client, resource.url)
        assert not err

        manifest, _, err = strip(k8sconfig, manifest, {})
        assert not err
    except AssertionError:
        return (MetaManifest("", "", "", ""), {}, True)

    return (make_meta(manifest), manifest, False)
