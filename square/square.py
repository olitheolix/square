import json
import logging
import os
from typing import Any, Iterable, Optional, Set, Tuple

import colorama
import jsonpatch
import square.k8s as k8s
import square.manio as manio
import yaml
from square.dtypes import (
    Config, DeltaCreate, DeltaDelete, DeltaPatch, DeploymentPlan, Filepath,
    JsonPatch, ManifestHierarchy, MetaManifest, Selectors, ServerManifests,
)

# Convenience: global logger instance to avoid repetitive code.
logit = logging.getLogger("square")


def make_patch(
        config: Config,
        local: ServerManifests,
        server: ServerManifests) -> Tuple[Optional[JsonPatch], bool]:
    """Return JSON patch to transition `server` to `local`.

    Inputs:
        local: LocalManifests
            Usually the dictionary keys returned by `load_manifest`.
        server: ServerManifests
            Usually the dictionary keys returned by `manio.download`.

    Returns:
        Patch: the JSON patch and human readable diff in a `Patch` tuple.

    """
    # Reduce local and server manifests to salient fields (ie apiVersion, kind,
    # metadata and spec). Abort on error.
    loc, err1 = manio.strip(config, local)
    srv, err2 = manio.strip(config, server)
    if err1 or err2 or loc is None or srv is None:
        return (None, True)

    # Log the manifest info for which we will try to compute a patch.
    man_id = f"{loc.kind.upper()}: {loc.metadata.name}/{loc.metadata.name}"
    logit.debug(f"Making patch for {man_id}")

    # Sanity checks: abort if the manifests do not specify the same resource.
    try:
        assert srv.apiVersion == loc.apiVersion
        assert srv.kind == loc.kind
        assert srv.metadata.name == loc.metadata.name

        # Not all resources live in a namespace, for instance Namespaces,
        # ClusterRoles, ClusterRoleBindings. Here we ensure that the namespace
        # in the local and server manifest matches for those resources that
        # have a namespace.
        if srv.kind in {"Namespace", "ClusterRole", "ClusterRoleBinding"}:
            namespace = None
        else:
            assert srv.metadata.namespace == loc.metadata.namespace
            namespace = srv.metadata.namespace
    except AssertionError:
        # Log the invalid manifests and return with an error.
        keys = ("apiVersion", "kind", "metadata")
        loc_tmp = {k: loc[k] for k in keys}
        srv_tmp = {k: srv[k] for k in keys}
        logit.error(
            "Cannot compute JSON patch for incompatible manifests. "
            f"Local: <{loc_tmp}>  Server: <{srv_tmp}>"
        )
        return (None, True)

    # Determine the PATCH URL for the resource.
    url, err = k8s.urlpath(config, srv.kind, namespace)
    if err:
        return (None, True)
    full_url = f'{url}/{srv.metadata.name}'

    # Compute JSON patch.
    patch = jsonpatch.make_patch(srv, loc)
    patch = json.loads(patch.to_string())

    # Return the patch.
    return (JsonPatch(full_url, patch), False)


def partition_manifests(
        local: ServerManifests,
        server: ServerManifests) -> Tuple[Optional[DeploymentPlan], bool]:
    """Compile `{local,server}` into CREATE, PATCH and DELETE groups.

    The returned deployment plan will contain *every* resource in
    `local` and `server` *exactly once*. Their relative
    order will also be preserved.

    Create: all resources that exist in `local` but not in `server`.
    Delete: all resources that exist in `server` but not in `local`.
    Patch : all resources that exist in both and therefore *may* need patching.

    Inputs:
        local: Dict[MetaManifest:str]
            Usually the dictionary keys returned by `load_manifest`.
        server: Dict[MetaManifest:str]
            Usually the dictionary keys returned by `manio.download`.

    Returns:
        DeploymentPlan

    """
    # Determine what needs adding, removing and patching to steer the K8s setup
    # towards what `local` specifies.
    loc = set(local.keys())
    srv = set(server.keys())
    create = loc - srv
    patch = loc.intersection(srv)
    delete = srv - loc

    # Convert the sets to list. Preserve the relative element ordering as it
    # was in `{local_server}`.
    create_l = [_ for _ in local if _ in create]
    patch_l = [_ for _ in local if _ in patch]
    delete_l = [_ for _ in server if _ in delete]

    # Return the deployment plan.
    plan = DeploymentPlan(create_l, patch_l, delete_l)
    return (plan, False)


def compile_plan(
        config: Config,
        local: ServerManifests,
        server: ServerManifests) -> Tuple[Optional[DeploymentPlan], bool]:
    """Return the `DeploymentPlan` to transition K8s to state of `local`.

    The deployment plan is a named tuple. It specifies which resources to
    create, patch and delete to ensure that the state of K8s matches that
    specified in `local`.

    Inputs:
        config: Config
        local: ServerManifests
            Should be output from `load_manifest` or `load`.
        server: ServerManifests
            Should be output from `manio.download`.

    Returns:
        DeploymentPlan

    """
    # Partition the set of meta manifests into create/delete/patch groups.
    plan, err = partition_manifests(local, server)
    if err or not plan:
        return (None, True)

    # Sanity check: the resources to patch *must* exist in both local and
    # server manifests. This is a bug if not.
    assert set(plan.patch).issubset(set(local.keys()))
    assert set(plan.patch).issubset(set(server.keys()))

    # Compile the Deltas to create the missing resources.
    create = []
    for delta in plan.create:
        url, err = k8s.urlpath(config, delta.kind, namespace=delta.namespace)
        if err:
            return (None, True)
        create.append(DeltaCreate(delta, url, local[delta]))

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
        url, err = k8s.urlpath(config, meta.kind, namespace=meta.namespace)
        if err:
            return (None, True)

        # DELETE requests must specify the resource name in the path.
        url = f"{url}/{meta.name}"

        # Assemble the meta and add it to the list.
        delete.append(DeltaDelete(meta, url, del_opts.copy()))

    # Iterate over each manifest that needs patching and determine the
    # necessary JSON Patch to transition K8s into the state specified in the
    # local manifests.
    patches = []
    for meta in plan.patch:
        # Compute textual diff (only useful for the user to study the diff).
        diff_str, err = manio.diff(config, local[meta], server[meta])
        if err:
            return (None, True)

        # Compute the JSON patch that will match K8s to the local manifest.
        patch, err = make_patch(config, local[meta], server[meta])
        if err:
            return (None, True)
        patches.append(DeltaPatch(meta, diff_str, patch))

    # Assemble and return the deployment plan.
    return (DeploymentPlan(create, patches, delete), False)


def print_deltas(plan: DeploymentPlan) -> Tuple[None, bool]:
    """Print human readable version of `plan` to terminal.

    Inputs:
        plan: DeploymentPlan

    Returns:
        None

    """
    # Terminal colours for convenience.
    cAdd = colorama.Fore.GREEN
    cDel = colorama.Fore.RED
    cMod = colorama.Fore.YELLOW
    cReset = colorama.Fore.RESET

    n_add, n_mod, n_del = 0, 0, 0

    # Use Green to list all the resources that we should create.
    for delta in plan.create:
        name = f"{delta.meta.kind.upper()} {delta.meta.namespace}/{delta.meta.name}"

        # Convert manifest to YAML string and print every line in Green.
        txt = yaml.dump(delta.manifest, default_flow_style=False)
        txt = [cAdd + line + cReset for line in txt.splitlines()]

        # Add header line.
        txt = [f"    {line}" for line in txt]
        txt.insert(0, cAdd + f"Create {name}" + cReset)

        # Print the reassembled string.
        print(str.join('\n', txt) + '\n')
        n_add += 1

    # Print the diff (already contains terminal colours) for all the resources
    # that we should patch.
    for delta in plan.patch:
        if len(delta.diff) == 0:
            continue

        # Add some terminal colours to make it look prettier.
        colour_lines = []
        for line in delta.diff.splitlines():
            if line.startswith('+'):
                colour_lines.append(cAdd + line + cReset)
            elif line.startswith('-'):
                colour_lines.append(cDel + line + cReset)
            else:
                colour_lines.append(line)
        colour_lines = [f"    {line}" for line in colour_lines]
        formatted_diff = str.join('\n', colour_lines)

        name = f"{delta.meta.kind.upper()} {delta.meta.namespace}/{delta.meta.name}"
        print(cMod + f"Patch {name}" + cReset + "\n" + formatted_diff + "\n")
        n_mod += 1

    # Use Red to list all the resources that we should delete.
    for delta in plan.delete:
        name = f"{delta.meta.kind.upper()} {delta.meta.namespace}/{delta.meta.name}"
        print(cDel + f"Delete {name}" + cReset)
        n_del += 1

    print("-" * 80)
    print("Plan: " +                         # noqa
          cAdd + f"{n_add:,} to add, " +     # noqa
          cMod + f"{n_mod:,} to change, " +  # noqa
          cDel + f"{n_del:,} to destroy." +  # noqa
          cReset + "\n")
    return (None, False)


def find_namespace_orphans(
        meta_manifests: Iterable[MetaManifest]
) -> Tuple[Optional[Set[MetaManifest]], bool]:
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

    # Extract all declared namespaces so we can find the orphans next.
    namespaces = {_.name for _ in meta_manifests if _.kind == 'Namespace'}

    # Find all manifests that are neither a Namespace nor belong to any of the
    # `namespaces` from the previous step
    orphans = {
        _ for _ in meta_manifests
        if _.kind != "Namespace" and _.namespace not in namespaces
    }

    # Return the result.
    return (orphans, True)


def setup_logging(log_level: int) -> None:
    """Configure logging at `log_level`.

    Level 0: ERROR
    Level 1: WARNING
    Level 2: INFO
    Level >=3: DEBUG

    Inputs:
        log_level: int

    Returns:
        None

    """
    class ColouredLog(logging.StreamHandler):
        def format(self, record):
            if record.levelname == "DEBUG":
                colour = colorama.Fore.WHITE
            elif record.levelname == "INFO":
                colour = colorama.Fore.GREEN
            else:
                colour = colorama.Fore.RED
            return f"{colour}{super().format(record)}{colorama.Fore.RESET}"

    # Pick the correct log level.
    if log_level == 0:
        level = "ERROR"
    elif log_level == 1:
        level = "WARNING"
    elif log_level == 2:
        level = "INFO"
    else:
        level = "DEBUG"

    # Create logger.
    logger = logging.getLogger("square")
    logger.setLevel(level)

    # Configure stdout handler.
    handler = ColouredLog()
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            "%(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s"
        )
    )

    # Attach stdout handlers to the `square` logger.
    logger.addHandler(handler)
    logit.info(f"Set log level to {level}")


def user_confirmed(answer: Optional[str] = "yes") -> bool:
    """Return True iff the user answers with `answer` or `answer` is None."""
    if answer is None:
        return True

    assert answer, "BUG: desired answer must be non-empty string or `None`"

    cDel = colorama.Fore.RED
    cReset = colorama.Fore.RESET

    # Confirm with user.
    print(f"Type {cDel}{answer}{cReset} to apply the plan.")
    try:
        return input("  Your answer: ") == answer
    except KeyboardInterrupt:
        print()
        return False


def main_apply(
        config: Config,
        client,
        folder: Filepath,
        selectors: Selectors,
        confirm_string: Optional[str]
) -> Tuple[None, bool]:
    """Update K8s to match the specifications in `local_manifests`.

    Create a deployment plan that will transition the K8s state
    `server_manifests` to the desired `local_manifests`.

    Inputs:
        config: Config
        client: `requests` session with correct K8s certificates.
        folder: Filepath
            Path to local manifests eg "./foo"
        selectors: Selectors
            Only operate on resources that match the selectors.
        confirm_string:
            Only apply the plan if user answers with this string in the
        confirmation dialog (set to `None` to disable confirmation).

    Returns:
        None

    """
    try:
        # Load manifests from local files.
        local_meta, _, err = manio.load(folder, selectors)
        assert not err and local_meta is not None

        # Download manifests from K8s.
        server, err = manio.download(config, client, selectors)
        assert not err and server is not None

        # Create the deployment plan.
        plan, err = compile_plan(config, local_meta, server)
        assert not err and plan is not None

        # Present the plan to the user.
        print_deltas(plan)

        # Exit prematurely if there are no changes to apply.
        num_patch_ops = sum([len(_.patch.ops) for _ in plan.patch])
        if len(plan.create) == len(plan.delete) == num_patch_ops == 0:
            print("Nothing to change")
            return (None, False)
        del num_patch_ops

        # Ask for user confirmation. Abort if the user does not give it.
        if not user_confirmed(confirm_string):
            print("User abort - no changes were made.")
            return (None, True)
        print()

        # Create the missing resources.
        for data in plan.create:
            print(f"Creating {data.meta.kind.upper()} "
                  f"{data.meta.namespace}/{data.meta.name}")
            _, err = k8s.post(client, data.url, data.manifest)
            assert not err

        # Patch the server resources.
        patches = [(_.meta, _.patch) for _ in plan.patch if len(_.patch.ops) > 0]
        for meta, patch in patches:
            print(f"Patching {meta.kind.upper()} "
                  f"{meta.namespace}/{meta.name}")
            _, err = k8s.patch(client, patch.url, patch.ops)
            assert not err

        # Delete the excess resources.
        for data in plan.delete:
            print(f"Deleting {data.meta.kind.upper()} "
                  f"{data.meta.namespace}/{data.meta.name}")
            _, err = k8s.delete(client, data.url, data.manifest)
            assert not err
    except AssertionError:
        return (None, True)

    # All good.
    return (None, False)


def main_plan(
        config: Config,
        client,
        folder: Filepath,
        selectors: Selectors,
) -> Tuple[None, bool]:
    """Print the diff between `local_manifests` and `server_manifests`.

    The diff shows what would have to change on the K8s server in order for it
    to match the setup defined in `local_manifests`.

    Inputs:
        config: k8s.Config
        client: `requests` session with correct K8s certificates.
        folder: Path
            Path to local manifests eg "./foo"
        selectors: Selectors
            Only operate on resources that match the selectors.

    Returns:
        None

    """
    try:
        # Load manifests from local files.
        local_meta, _, err = manio.load(folder, selectors)
        assert not err and local_meta is not None

        # Download manifests from K8s.
        server, err = manio.download(config, client, selectors)
        assert not err and server is not None

        # Create deployment plan.
        plan, err = compile_plan(config, local_meta, server)
        assert not err and plan
    except AssertionError:
        return (None, True)

    # Print the plan and return.
    print_deltas(plan)
    return (None, False)


def main_get(
        config: Config,
        client,
        folder: Filepath,
        selectors: Selectors,
        groupby: ManifestHierarchy,
) -> Tuple[None, bool]:

    """Download all K8s manifests and merge them into local files.

    Inputs:
        config: k8s.Config
        client: `requests` session with correct K8s certificates.
        folder: Path
            Path to local manifests eg "./foo"
        selectors: Selectors
            Only operate on resources that match the selectors.
        groupby: ManifestHierarchy
            Specify relationship between new manifests and file names.

    Returns:
        None

    """
    try:
        # Load manifests from local files.
        _, local_files, err = manio.load(folder, selectors)
        assert not err and local_files is not None

        # Download manifests from K8s.
        server, err = manio.download(config, client, selectors)
        assert not err and server is not None

        # Sync the server manifests into the local manifests. All this happens in
        # memory and no files will be modified here - see `manio.save` in the next step.
        synced_manifests, err = manio.sync(local_files, server, selectors, groupby)
        assert not err and synced_manifests

        # Write the new manifest files.
        _, err = manio.save(folder, synced_manifests)
        assert not err
    except AssertionError:
        return (None, True)

    # Success.
    return (None, False)


def cluster_config(
        kubeconfig: str,
        context: Optional[str]) -> Tuple[Tuple[Optional[Config], Any], bool]:
    """Return web session to K8s API.

    This will read the Kubernetes credentials, contact Kubernetes to
    interrogate its version and then return the configuration and web-session.

    Inputs:
        kubeconfig: str
            Path to kubeconfig file.
        kube_context: str
            Kubernetes context to use (can be `None` to use default).

    Returns:
        Config, client

    """
    # Read Kubeconfig file and use it to create a `requests` client session.
    # That session will have the proper security certificates and headers so
    # that subsequent calls to K8s need not deal with it anymore.
    kubeconfig = os.path.expanduser(kubeconfig)
    try:
        # Parse Kubeconfig file.
        config = k8s.load_auto_config(kubeconfig, context, disable_warnings=True)
        assert config

        # Configure web session.
        client = k8s.session(config)
        assert client

        # Contact the K8s API to update version field in `config`.
        config, err = k8s.version(config, client)
        assert not err and config
    except AssertionError:
        return ((None, None), True)

    # Log the K8s API address and version.
    logit.info(f"Kubernetes server at {config.url}")
    logit.info(f"Kubernetes version is {config.version}")
    return (config, client), False
