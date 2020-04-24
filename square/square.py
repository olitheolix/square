import json
import logging
from typing import Iterable, Optional, Set, Tuple

import colorama
import jsonpatch
import square.dtypes as dtypes
import square.k8s as k8s
import square.manio as manio
import yaml
from colorlog import ColoredFormatter
from square.dtypes import (
    DeltaCreate, DeltaDelete, DeltaPatch, DeploymentPlan, DeploymentPlanMeta,
    Filepath, GroupBy, JsonPatch, K8sConfig, MetaManifest, Selectors,
    ServerManifests,
)

# Convenience: global logger instance to avoid repetitive code.
logit = logging.getLogger("square")


def make_patch(
        config: K8sConfig,
        local: ServerManifests,
        server: ServerManifests) -> Tuple[JsonPatch, bool]:
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
    (loc, _), err1 = manio.strip(config, local)
    (srv, _), err2 = manio.strip(config, server)
    if err1 or err2 or loc is None or srv is None:
        return (JsonPatch("", []), True)

    # Log the manifest info for which we will try to compute a patch.
    man_id = f"{loc.kind.upper()}: {loc.metadata.name}/{loc.metadata.name}"
    logit.debug(f"Making patch for {man_id}")

    # Sanity checks: abort if the manifests do not specify the same resource.
    try:
        res_srv, err_srv = k8s.resource(config, manio.make_meta(srv))
        res_loc, err_loc = k8s.resource(config, manio.make_meta(loc))
        assert err_srv is err_loc is False
        assert res_srv == res_loc
    except AssertionError:
        # Log the invalid manifests and return with an error.
        keys = ("apiVersion", "kind", "metadata")
        loc_tmp = {k: loc[k] for k in keys}
        srv_tmp = {k: srv[k] for k in keys}
        logit.error(
            "Cannot compute JSON patch for incompatible manifests. "
            f"Local: <{loc_tmp}>  Server: <{srv_tmp}>"
        )
        return (JsonPatch("", []), True)

    # Compute JSON patch.
    patch = jsonpatch.make_patch(srv, loc)
    patch = json.loads(patch.to_string())

    # Return the patch.
    return (JsonPatch(res_srv.url, patch), False)


def partition_manifests(
        local: ServerManifests,
        server: ServerManifests) -> Tuple[DeploymentPlanMeta, bool]:
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
        DeploymentPlanMeta

    """
    # Determine what needs adding, removing and patching to steer the K8s setup
    # towards what `local` specifies.
    meta_loc = set(local.keys())
    meta_srv = set(server.keys())
    create = meta_loc - meta_srv
    patch = meta_loc.intersection(meta_srv)
    delete = meta_srv - meta_loc
    del meta_loc, meta_srv

    # Convert the sets to list. Preserve the relative element ordering as it
    # was in `{local_server}`.
    create_l = [_ for _ in local if _ in create]
    patch_l = [_ for _ in local if _ in patch]
    delete_l = [_ for _ in server if _ in delete]

    # Return the deployment plan.
    plan = DeploymentPlanMeta(create_l, patch_l, delete_l)
    return (plan, False)


def preferred_api(config: K8sConfig,
                  local: ServerManifests) -> Tuple[ServerManifests, bool]:
    """Return a new version of `local` where all APIs point to the preferred group.

    Example:
      in = {
        MetaManifest(apiVersion='extensions/v1beta1', kind='Ingress', ...): {
          'apiVersion': 'networking.k8s.io/v1beta1',
          ...
        }
      }
      out = {
        MetaManifest(apiVersion='networking.k8s.io/v1beta1', kind='Ingress', ...): {
          'apiVersion': 'networking.k8s.io/v1beta1',
          ...
        }
      }

    """
    # Check the API version in each MetaManifest and update it to the preferred
    # version in both the MetaManifest and the associated K8s YAML manifest.
    out = {}
    for meta, manifest in local.items():
        # The current `meta` can be outdated but must still be a valid resource
        # in the current cluster.
        if k8s.resource(config, meta)[1]:
            return {}, True

        # Get the canonical resource description.
        res, err = k8s.resource(config, meta._replace(apiVersion=""))
        if err:
            logit.critical(f"BUG: resource <{meta}> should have been valid")
            return {}, True

        # Log a warning if the manifests uses an outdated API group.
        if meta.apiVersion != res.apiVersion:
            logit.warning(
                f"Switching <{res.kind} {meta.namespace}/{meta.name}>"
                f" from <{meta.apiVersion}> to <{res.apiVersion}>"
                " - Please update your manifests."
            )

            # Update the API version for the resource to whatever K8s prefers.
            meta = meta._replace(apiVersion=res.apiVersion)
            manifest["apiVersion"] = meta.apiVersion
        out[meta] = manifest
    return out, False


def compile_plan(
        config: K8sConfig,
        local: ServerManifests,
        server: ServerManifests) -> Tuple[DeploymentPlan, bool]:
    """Return the `DeploymentPlan` to transition K8s to state of `local`.

    The deployment plan is a named tuple. It specifies which resources to
    create, patch and delete to ensure that the state of K8s matches that
    specified in `local`.

    Inputs:
        config: K8sConfig
        local: ServerManifests
            Should be output from `load_manifest` or `load`.
        server: ServerManifests
            Should be output from `manio.download`.

    Returns:
        DeploymentPlan

    """
    err_resp = (DeploymentPlan(tuple(), tuple(), tuple()), True)

    # Replace the API group of the local resource with the one K8s prefers.
    local, err = preferred_api(config, local)
    if err:
        return err_resp

    # Partition the set of meta manifests into create/delete/patch groups.
    plan, err = partition_manifests(local, server)
    if err or not plan:
        return err_resp

    # Sanity check: the resources to patch *must* exist in both local and
    # server manifests. This is a bug if not.
    assert set(plan.patch).issubset(set(local.keys()))
    assert set(plan.patch).issubset(set(server.keys()))

    # Compile the Deltas to create the missing resources.
    create = []
    for delta in plan.create:
        # We only need the resource and namespace, not its name, because that
        # is how the POST request to create a resource works in K8s.
        resource, err = k8s.resource(config, delta._replace(name=""))
        if err or not resource:
            return err_resp
        create.append(DeltaCreate(delta, resource.url, local[delta]))

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
        resource, err = k8s.resource(config, meta)
        if err or not resource:
            return err_resp

        # Compile the Delta and add it to the list.
        delete.append(DeltaDelete(meta, resource.url, del_opts.copy()))

    # Iterate over each manifest that needs patching and determine the
    # necessary JSON Patch to transition K8s into the state specified in the
    # local manifests.
    patches = []
    for meta in plan.patch:
        # Compute textual diff (only useful for the user to study the diff).
        diff_str, err = manio.diff(config, local[meta], server[meta])
        if err or diff_str is None:
            return err_resp

        # Compute the JSON patch that will change the K8s state to match the
        # one in the local files.
        patch, err = make_patch(config, local[meta], server[meta])
        if err or patch is None:
            return err_resp

        # Append the patch to the list of patches, unless it is empty.
        if len(patch.ops):
            patches.append(DeltaPatch(meta, diff_str, patch))

    # Assemble and return the deployment plan.
    return (DeploymentPlan(create, patches, delete), False)


def show_plan(plan: Optional[DeploymentPlan]) -> bool:
    """Print human readable version of `plan` to terminal.

    Inputs:
        plan: DeploymentPlan

    Returns:
        None

    """
    # Do nothing if the plan is `None`. This special case makes it easier to
    # deal with cases where `square.make_plan` returns an error.
    if not plan:
        return False

    # Terminal colours for convenience.
    cAdd = colorama.Fore.GREEN
    cDel = colorama.Fore.RED
    cMod = colorama.Fore.YELLOW
    cReset = colorama.Fore.RESET

    n_add, n_mod, n_del = 0, 0, 0

    # Use Green to list all the resources that we should create.
    for delta_c in plan.create:
        name = f"{delta_c.meta.kind.upper()} {delta_c.meta.namespace}/{delta_c.meta.name}"
        name += f" ({delta_c.meta.apiVersion})"

        # Convert manifest to YAML string and print every line in Green.
        txt = yaml.dump(delta_c.manifest, default_flow_style=False)
        txt = [cAdd + line + cReset for line in txt.splitlines()]

        # Add header line.
        txt = [f"    {line}" for line in txt]
        txt.insert(0, cAdd + f"Create {name}" + cReset)

        # Print the reassembled string.
        print(str.join('\n', txt) + '\n')
        n_add += 1

    # Print the diff (already contains terminal colours) for all the resources
    # that we should patch.
    for delta_p in plan.patch:
        if len(delta_p.diff) == 0:
            continue

        # Add some terminal colours to make it look prettier.
        colour_lines = []
        for line in delta_p.diff.splitlines():
            if line.startswith('+'):
                colour_lines.append(cAdd + line + cReset)
            elif line.startswith('-'):
                colour_lines.append(cDel + line + cReset)
            else:
                colour_lines.append(line)
        colour_lines = [f"    {line}" for line in colour_lines]
        formatted_diff = str.join('\n', colour_lines)

        name = f"{delta_p.meta.kind.upper()} {delta_p.meta.namespace}/{delta_p.meta.name}"
        name += f" ({delta_p.meta.apiVersion})"
        print(cMod + f"Patch {name}" + cReset + "\n" + formatted_diff + "\n")
        n_mod += 1

    # Use Red to list all the resources that we should delete.
    for delta_d in plan.delete:
        name = f"{delta_d.meta.kind.upper()} {delta_d.meta.namespace}/{delta_d.meta.name}"
        name += f" ({delta_d.meta.apiVersion})"
        print(cDel + f"Delete {name}" + cReset)
        n_del += 1

    print("-" * 80)
    print("Plan: " +                         # noqa
          cAdd + f"{n_add:,} to add, " +     # noqa
          cMod + f"{n_mod:,} to change, " +  # noqa
          cDel + f"{n_del:,} to destroy." +  # noqa
          cReset + "\n")
    return False


def find_namespace_orphans(
        meta_manifests: Iterable[MetaManifest]
) -> Tuple[Set[MetaManifest], bool]:
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
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(
        ColoredFormatter(
            "%(log_color)s%(levelname)s%(reset)s - "
            "%(filename)s:%(funcName)s:%(lineno)d - %(message)s"
        )
    )

    # Attach stdout handlers to the `square` logger.
    logger.addHandler(handler)
    logit.info(f"Set log level to {level}")


def apply_plan(
        kubeconfig: Filepath,
        kube_ctx: Optional[str],
        plan: DeploymentPlan) -> bool:
    """Update K8s resources according to the `plan`.

    Inputs:
        kubeconfig: Filepath,
        kube_ctx: Optional[str],
        plan: DeploymentPlan

    Returns:
        None

    """
    try:
        # Create properly configured Requests session to talk to K8s API.
        k8s_config, k8s_client, err = k8s.cluster_config(kubeconfig, kube_ctx)
        assert not err and k8s_config and k8s_client

        # Create the missing resources.
        for data_c in plan.create:
            print(f"Creating {data_c.meta.kind.upper()} "
                  f"{data_c.meta.namespace}/{data_c.meta.name}")
            _, err = k8s.post(k8s_client, data_c.url, data_c.manifest)
            assert not err

        # Patch the server resources.
        patches = [(_.meta, _.patch) for _ in plan.patch if len(_.patch.ops) > 0]
        for meta, patch in patches:
            print(f"Patching {meta.kind.upper()} "
                  f"{meta.namespace}/{meta.name}")
            _, err = k8s.patch(k8s_client, patch.url, patch.ops)
            assert not err

        # Delete the excess resources.
        for data_d in plan.delete:
            print(f"Deleting {data_d.meta.kind.upper()} "
                  f"{data_d.meta.namespace}/{data_d.meta.name}")
            _, err = k8s.delete(k8s_client, data_d.url, data_d.manifest)
            assert not err
    except AssertionError:
        return True

    # All good.
    return False


def make_plan(
        kubeconfig: Filepath,
        kube_ctx: Optional[str],
        folder: Filepath,
        selectors: Selectors,
) -> Tuple[DeploymentPlan, bool]:
    """Print the diff between `local_manifests` and `server_manifests`.

    The diff shows what would have to change on the K8s server in order for it
    to match the setup defined in `local_manifests`.

    Inputs:
        kubeconfig: Filepath
            Path to Kubernetes credentials.
        kubectx: str
            Kubernetes context (use `None` to use the default).
        client: `requests` session with correct K8s certificates.
        folder: Path
            Path to local manifests eg "./foo"
        selectors: Selectors
            Only operate on resources that match the selectors.

    Returns:
        Deployment plan.

    """
    try:
        # Create properly configured Requests session to talk to K8s API.
        k8s_config, k8s_client, err = k8s.cluster_config(kubeconfig, kube_ctx)
        assert not err

        # Load manifests from local files.
        local, _, err = manio.load(folder, selectors)
        assert not err and local is not None

        # Download manifests from K8s.
        server, err = manio.download(k8s_config, k8s_client, selectors)
        assert not err and server is not None

        # Align non-plannable fields, like the ServiceAccount tokens.
        local_meta, err = manio.align_serviceaccount(local, server)
        assert not err

        # Create deployment plan.
        plan, err = compile_plan(k8s_config, local_meta, server)
        assert not err and plan
    except AssertionError:
        return (DeploymentPlan(tuple(), tuple(), tuple()), True)

    # Print the plan and return.
    return (plan, False)


def get_resources(
        kubeconfig: Filepath,
        kube_ctx: Optional[str],
        folder: Filepath,
        selectors: Selectors,
        groupby: GroupBy) -> bool:
    """Download all K8s manifests and merge them into local files.

    Inputs:
        kubeconfig: Filepath
            Path to Kubernetes credentials.
        kubectx: str
            Kubernetes context (use `None` to use the default).
        folder: Path
            Path to local manifests eg "./foo"
        selectors: Selectors
            Only operate on resources that match the selectors.
        groupby: GroupBy
            Specify relationship between new manifests and file names.

    Returns:
        None

    """
    try:
        # Create properly configured Requests session to talk to K8s API.
        k8s_config, k8s_client, err = k8s.cluster_config(kubeconfig, kube_ctx)
        assert not err and k8s_config and k8s_client

        # Use a wildcard Selector to ensure `manio.load` will read _all_ local
        # manifests. This will allow `manio.sync` to modify the ones specified by
        # the `selector` argument only, delete all the local manifests, and then
        # write the new ones. This logic will ensure we never have stale manifests
        # (see `manio.save_files` for details and how `manio.save`, which we call
        # at the end of this function, uses it).
        load_selectors = Selectors(kinds=k8s_config.kinds,
                                   labels=None,
                                   namespaces=None)

        # Load manifests from local files.
        _, local_files, err = manio.load(folder, load_selectors)
        assert not err and local_files is not None

        # Download manifests from K8s.
        server, err = manio.download(k8s_config, k8s_client, selectors)
        assert not err and server is not None

        # Sync the server manifests into the local manifests. All this happens in
        # memory and no files will be modified here - see `manio.save` in the next step.
        synced_manifests, err = manio.sync(local_files, server, selectors,
                                           groupby, k8s_config.kinds)
        assert not err and synced_manifests

        # Write the new manifest files.
        err = manio.save(folder, synced_manifests, tuple(dtypes.SUPPORTED_KINDS))
        assert not err
    except AssertionError:
        return True

    # Success.
    return False
