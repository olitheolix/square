import copy
import json
import logging
import re
import sys
import traceback
from collections import Counter
from typing import Collection, Dict, List, Optional, Set, Tuple

import colorama
import jsonpatch
import yaml
from colorlog import ColoredFormatter

import square.dotdict as dotdict
import square.k8s as k8s
import square.manio as manio
from square.dtypes import (
    Config, DeltaCreate, DeltaDelete, DeltaPatch, DeploymentPlan,
    DeploymentPlanMeta, JsonPatch, K8sConfig, MetaManifest, Selectors,
    SquareManifests,
)

# Convenience: global logger instance to avoid repetitive code.
logit = logging.getLogger("square")


def translate_resource_kinds(cfg: Config, k8sconfig: K8sConfig) -> Config:
    """Convert `cfg.Selectors.kind` and `cfg.priorities` to their canonical names.

    Example: "svc" -> "Service" or "ns" -> "Namespace".

    Silently ignore unknown resource kinds. This is necessary because the user
    may have specified a custom resource that does not (yet) exist. Since we
    cannot distinguish those from typos we allow them here because the
    get/plan/apply cycle will ignore them anyway.

    """
    # Convenience
    short2kind = k8sconfig.short2kind

    # Avoid side effects to the original `cfg`.
    cfg = copy.deepcopy(cfg)

    # Translate the shorthand names to their canonical K8s names. Use the
    # original name if we cannot find a canonical name for it.
    cfg.priorities = [short2kind.get(_.lower(), _) for _ in cfg.priorities]

    # Backup the original list of KIND selectors.
    kinds_names = [(_.kind, _.name) for _ in cfg.selectors._kinds_names]
    cfg.selectors.kinds.clear()

    # Convert eg [("ns"), ("svc", "app1")] -> {"Namespace", "Service/app1"}.
    for kind, name in kinds_names:
        ans = short2kind.get(kind.lower(), kind)
        ans = ans if name == "" else f"{ans}/{name}"
        cfg.selectors.kinds.add(ans)

    return cfg


def make_patch(
        k8sconfig: K8sConfig,
        local: dict,
        server: dict) -> Tuple[JsonPatch, bool]:
    """Return JSON patch to transition `server` to `local`.

    Inputs:
        k8sconfig: K8sConfig
        local: dict
            Usually one of the manifests returned by `load_manifest`.
        server: dict
            Usually one of the manifests returned by `manio.download`.

    Returns:
        Patch: the JSON patch and human readable diff in a `Patch` tuple.

    """
    # Convenience.
    loc, srv = local, server
    meta = manio.make_meta(local)

    # Log the manifest info for which we will try to compute a patch.
    man_id = f"{meta.kind}: {meta.namespace}/{meta.name}"
    logit.debug(f"Making patch for {man_id}")
    del meta

    # Sanity checks: abort if the manifests do not specify the same resource.
    try:
        res_srv, err_srv = k8s.resource(k8sconfig, manio.make_meta(srv))
        res_loc, err_loc = k8s.resource(k8sconfig, manio.make_meta(loc))
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
        local: SquareManifests,
        server: SquareManifests) -> Tuple[DeploymentPlanMeta, bool]:
    """Compile `{local,server}` into CREATE, PATCH and DELETE groups.

    The returned deployment plan will contain *every* resource in
    `local` and `server` *exactly once*. Their relative
    order will also be preserved.

    Create: all resources that exist in `local` but not in `server`.
    Delete: all resources that exist in `server` but not in `local`.
    Patch : all resources that exist in both and therefore *may* need patching.

    Inputs:
        local: SquareManifests
            Usually the dictionaries returned by `load_manifest`.
        server: SquareManifests
            Usually the dictionaries returned by `manio.download`.

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


def match_api_version(
        k8sconfig: K8sConfig,
        local: SquareManifests,
        server: SquareManifests) -> Tuple[SquareManifests, bool]:
    """Fetch the manifests from the endpoints defined in the local manifest.

    If a local manifest uses a different value for `apiVersion` then we need to
    re-fetch those manifests from K8s via that endpoint. This function does
    just that.

    This function returns `server` verbatim if there is no overlap with
    `server` and `local`.

    Inputs:
        config: Square configuration.
        k8sconfig: K8sConfig
        local: SquareManifests
            Should be output from `load_manifest` or `load`.
        server: SquareManifests
            Should be output from `manio.download`.

    Returns:
        `server` but possibly with some entries re-fetched from the same K8s
        endpoint that the equivalent resource in `local` specifies.

    """
    # Avoid side effects.
    server = copy.deepcopy(server)

    # Find the resources that exist in local manifests and on K8s. The
    # resources are identical if their MetaManifest are identical save for the
    # `apiVersion` field.
    mm_loc = {meta._replace(apiVersion=""): meta for meta in local}
    mm_srv = {meta._replace(apiVersion=""): meta for meta in server}
    meta_overlap = set(mm_loc.keys()) & set(mm_srv.keys())

    # Iterate over all the resources that exist on both the server and locally,
    # even though they may use different API versions.
    to_download: List[MetaManifest] = []
    for meta in meta_overlap:
        # Lookup the full MetaManifest for the local and server resource.
        # NOTE: meta_{loc,srv} are identical except possibly for the `apiVersion` field.
        meta_loc = mm_loc[meta]
        meta_srv = mm_srv[meta]

        # Do nothing if the `apiVersions` match because we can already compute a
        # plan for it. However, if the `apiVersions` differ then we will
        # replace entry in `server` with the one fetched from the correct K8s
        # endpoint (see next section).
        if meta_loc != meta_srv:
            del server[meta_srv]
            to_download.append(meta_loc)
            logit.info(
                f"Using non-default {meta.kind.upper()} endpoint "
                f"<{meta_loc.apiVersion}>"
            )

    # Re-fetch the resources we already got but this time from the correct endpoint.
    for meta in to_download:
        # Construct the correct K8sResource.
        resource, err = k8s.resource(k8sconfig, meta)
        assert not err

        # Download all resources of the current kind.
        meta, manifest, err = manio.download_single(k8sconfig, resource)
        assert not err

        # Add the resource to the `server` dict. This will have been one of
        # those we deleted a few lines earlier.
        server[meta] = manifest

    return server, False


def run_user_callback(config: Config,
                      plan_patch: List[MetaManifest],
                      local: Dict[MetaManifest, dict],
                      server: Dict[MetaManifest, dict]) -> bool:
    """Run the user supplied callback function for all manifests to patch.

    This function will run *after* the filters from the `Config` were applied.

    NOTE: modifies `local` and `server` inplace.

    Inputs:
        config: Square configuration.
        plan_patch: List[MetaManifest]
            The list of meta manifests that currently require a patch.
        local: SquareManifests
            Should be output from `load_manifest` or `load`.
        server: SquareManifests
            Should be output from `manio.download`.

    """
    # Do nothing and return immediately if the user did not provide a callback.
    cb = config.patch_callback
    if not cb:
        return False

    # Run user supplied callback for each local/server manifest pair that needs
    # patching. This will update our dict of local/server manifests inplace.
    try:
        for meta in plan_patch:
            local[meta], server[meta] = cb(config, local[meta], server[meta])
    except Exception:
        # Error in user supplied callback function. Print a hopefully useful
        # stack trace and the return cleanly with an error.
        tb_str = str.join(
            "\n",
            traceback.format_exception(*sys.exc_info())
        )
        logit.error(
            "Error in filter callback. See below for details:\n"
            "---\n"
            f"{tb_str}---"
        )
        return True
    return False


def compile_plan(
        config: Config,
        k8sconfig: K8sConfig,
        local: SquareManifests,
        server: SquareManifests) -> Tuple[DeploymentPlan, bool]:
    """Return the `DeploymentPlan` to transition K8s to the `local` state.

    The deployment plan is a named tuple. It specifies which resources to
    create, patch and delete to ensure that the state of K8s matches that
    specified in `local`.

    Inputs:
        config: Square configuration.
        k8sconfig: K8sConfig
        local: SquareManifests
            Should be output from `load_manifest` or `load`.
        server: SquareManifests
            Should be output from `manio.download`.

    Returns:
        DeploymentPlan

    """
    err_resp = (DeploymentPlan(tuple(), tuple(), tuple()), True)

    # Abort unless all local manifests reference valid K8s resource kinds.
    if any([k8s.resource(k8sconfig, meta)[1] for meta in local]):
        return err_resp

    # Replace the server resources fetched from K8s' preferred endpoint with
    # those from the endpoint declared in the local manifest.
    server, err = match_api_version(k8sconfig, local, server)
    assert not err

    # Strip the unwanted sections from the manifests before we compute patches.
    stripped_server = {
        meta: manio.strip(k8sconfig, man, config.filters)
        for meta, man in server.items()
    }
    stripped_local = {
        meta: manio.strip(k8sconfig, man, config.filters)
        for meta, man in local.items()
    }

    # Abort if any of the manifests could not be stripped.
    err_srv = {_[2] for _ in stripped_server.values()}
    err_loc = {_[2] for _ in stripped_local.values()}
    if any(err_srv) or any(err_loc):
        logit.error("Could not strip all manifests.")
        return err_resp

    # Unpack the stripped manifests (ie first element in the tuple returned
    # by `manio.strip`).
    server = {k: dotdict.undo(v[0]) for k, v in stripped_server.items()}
    local = {k: dotdict.undo(v[0]) for k, v in stripped_local.items()}

    # Partition the set of meta manifests into create/delete/patch groups.
    plan, err = partition_manifests(local, server)
    if err:
        logit.error("Could not partition the manifests for the plan.")
        return err_resp

    # Sanity check: the resources to patch *must* exist in both local and
    # server manifests. If not, we have a bug.
    assert set(plan.patch).issubset(set(local.keys()))
    assert set(plan.patch).issubset(set(server.keys()))

    # For later: every DELETE request will have to pass along a `DeleteOptions`
    # manifest (see below).
    del_opts = {
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "gracePeriodSeconds": 0,
        "orphanDependents": False,
    }

    # Compile the Deltas to create the missing resources.
    create = []
    for delta in plan.create:
        # We only need the resource and namespace, not its name, because that
        # is how the POST request to create a resource works in K8s.
        # Ignore the error flag because the `strip` function we used above
        # already ensured the resource exists.
        resource, err = k8s.resource(k8sconfig, delta._replace(name=""))
        assert not err

        # Compile the Delta and add it to the list.
        create.append(DeltaCreate(delta, resource.url, local[delta]))

    # Compile the Deltas to delete the excess resources.
    delete = []
    for meta in plan.delete:
        # Resource URL. Ignore the error flag because `strip` already called
        # `k8s.resource` earlier and would have aborted if there was an error.
        resource, err = k8s.resource(k8sconfig, meta)
        assert not err

        # Compile the Delta and add it to the list.
        delete.append(DeltaDelete(meta, resource.url, del_opts.copy()))

    # Run the local/server manifests through a custom callback function that
    # can modify them before Square computes the patch.
    if run_user_callback(config, list(plan.patch), local, server):
        return err_resp

    # Iterate over each manifest that needs patching and determine the
    # necessary JSON Patch to transition K8s into the state specified in the
    # local manifests.
    patches = []
    for meta in plan.patch:
        # Compute human readable diff.
        diff_str, err = manio.diff(local[meta], server[meta])
        if err or diff_str is None:
            logit.error(f"Could not compute the diff for <{meta}>.")
            return err_resp

        # Compute the JSON patch that will change the K8s state to match the
        # one in the local files.
        patch, err = make_patch(k8sconfig, local[meta], server[meta])
        if err or patch is None:
            logit.error(f"Could not compute the patch for <{meta}>")
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
    cMod = colorama.Fore.YELLOW + colorama.Style.BRIGHT
    cDel = colorama.Fore.RED
    cReset = colorama.Fore.RESET + colorama.Style.RESET_ALL

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

    # Only use color if a category (ie to ADD, MODIFY or DELETE) is nonzero.
    cAdd = cAdd if len(plan.create) else colorama.Style.BRIGHT + colorama.Fore.WHITE
    cMod = cMod if len(plan.patch) else colorama.Style.BRIGHT + colorama.Fore.WHITE
    cDel = cDel if len(plan.delete) else colorama.Style.BRIGHT + colorama.Fore.WHITE

    print("-" * 80)
    print("Plan: " +                         # noqa
          cReset + cAdd + f"{n_add:,} to add, " +     # noqa
          cReset + cMod + f"{n_mod:,} to change, " +  # noqa
          cReset + cDel + f"{n_del:,} to destroy." +  # noqa
          cReset + "\n")
    return False


def find_namespace_orphans(
        meta_manifests: Collection[MetaManifest]
) -> Tuple[Set[MetaManifest], bool]:
    """Return all orphaned resources in the `meta_manifest` set.

    A resource is orphaned iff it lives in a namespace that is not explicitly
    declared in the set of `meta_manifests`.

    This function is particularly useful to verify a set of local manifests and
    pinpoint resources that someone forgot to delete and that have a typo in
    their namespace field.

    Inputs:
        meta_manifests: Collection[MetaManifest]

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


def sort_plan(cfg: Config, plan: DeploymentPlan) -> Tuple[DeploymentPlan, bool]:
    """Return a copy of the `plan` where the entries are sorted by priority.

    For example, if `cfg.priorities = ["Namespace", "Deployment"]` then
    `plan.create` will first list all Namespace resources, then "Deployment"
    resources, then everything else. The same applies to `plan.delete` except
    the list will be reversed, ie the top entries are the ones missing from
    `cfg.priorities`, followed by "Deployments", followed by "Namespaces".

    """
    # Return with an error if the entries in `cfg.priorities` are not unique.
    if len(set(cfg.priorities)) < len(cfg.priorities):
        duplicates = {k for k, v in Counter(cfg.priorities).items() if v > 1}
        logit.error(f"Found duplicates in the priorities: {duplicates}")
        return plan, True

    # -------------------------------------------------------------------------
    # The algorithm proceeds as follows: assign in `cfg.priorities` a value
    # (resources with a higher value have a lower priority). Then it will
    # prefix the tuples in `plan.{create,delete}` with the priority ID and sort
    # the new list. This will ensure the all resources appear in the order
    # defined in `cfg.priorities`. Resources with the same priority will be
    # sorted by MetaManifest, which means sorted by namespace, the name.
    # -------------------------------------------------------------------------

    # All unknown resource kinds will have this priority, which is larger (ie
    # less important) than all the resource kinds that are in `cfg.priorities`.
    max_id = len(cfg.priorities)

    # Assign each resource kind a number in order of priority.
    priority_id = {name: idx for idx, name in enumerate(cfg.priorities)}

    # Assign all resource kinds that exist in `plan.{create,delete}` a priority
    # number that is larger than all other numbers in that list. This will
    # ensure they come last when we sort.
    missing_create = {_.meta.kind for _ in plan.create if _.meta.kind not in priority_id}
    priority_id.update({kind: max_id for kind in missing_create})
    missing_delete = {_.meta.kind for _ in plan.delete if _.meta.kind not in priority_id}
    priority_id.update({kind: max_id for kind in missing_delete})

    # Sort the patches by priority ID and MetaManifest.
    create = [(priority_id[_.meta.kind], _) for _ in plan.create]
    create.sort(key=lambda _: _[:2])

    # Repeat for the patches that will delete resources but reverse the final
    # list. This will ensure we remove resources in the reverse order in which
    # we would create them.
    delete = [(priority_id[_.meta.kind], _) for _ in plan.delete]
    delete.sort(key=lambda _: _[:2])
    delete.reverse()

    # Assemble the final deployment plan and return it.
    out = DeploymentPlan(
        create=[_[1] for _ in create],
        patch=list(plan.patch),
        delete=[_[1] for _ in delete],
    )
    return out, False


def valid_label(label: str) -> bool:
    """Return `True` if `label` is K8s and Square compatible.

    The `label` contains both key and value, eg `app=square`.

    """
    # K8s uses this regex to validate the three individual label components
    # `name`, `part` and `suffix`.
    # Example: "name=part/suffix" -> ("name", "part", "value").
    pat = re.compile(r'^([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]$')

    try:
        # Split `app=part/file` into (`app`, `part/value`).
        assert label.count("=") == 1
        tmp, value = label.split("=")

        # Split on the "/"
        # "part/value" -> ("part", "value")
        # "value" -> ("", "value")
        part, _, name = tmp.rpartition("/")
        del label, _

        # If the label contains a "/" then validate its "part" component.
        assert len(name) > 0
        if "/" in tmp:
            assert pat.match(part)

        # All components must be at most 64 characters long.
        assert max(len(name), len(part), len(value)) < 64

        # Validate the label and value.
        assert pat.match(name)
        assert pat.match(value)

        # The label is valid.
        return True
    except AssertionError:
        return False


def apply_plan(cfg: Config, plan: DeploymentPlan) -> bool:
    """Update K8s resources according to the `plan`.

    Inputs:
        cfg: Square configuration.
        plan: DeploymentPlan

    Returns:
        None

    """
    # Sanity check labels.
    if not all([valid_label(_) for _ in cfg.selectors.labels]):
        logit.error(f"Invalid labels: {cfg.selectors.labels}")
        return True

    # Sort the plan according to `cfg.priority`.
    plan, plan_err = sort_plan(cfg, plan)

    # Create properly configured "Requests" session to talk to the K8s API.
    k8sconfig, k8s_err = k8s.cluster_config(cfg.kubeconfig, cfg.kubecontext)

    # Abort if we could not get the plan or establish the K8s session.
    if plan_err or k8s_err:
        return True

    # Convert "Selectors.kinds" to their canonical names.
    cfg = translate_resource_kinds(cfg, k8sconfig)

    # Create the missing resources. Abort on first error.
    for data_c in plan.create:
        msg_res = f"{data_c.meta.kind.upper()} {data_c.meta.namespace}/{data_c.meta.name}"
        print(f"Creating {msg_res}")
        _, err = k8s.post(k8sconfig.client, data_c.url, data_c.manifest)
        if err:
            logit.error(f"Could not patch {msg_res}")
            return True

    # Patch the server resources. Abort on first error.
    patches = [(_.meta, _.patch) for _ in plan.patch if len(_.patch.ops) > 0]
    for meta, patch in patches:
        msg_res = f"{meta.kind.upper()} {meta.namespace}/{meta.name}"
        print(f"Patching {msg_res}")
        _, err = k8s.patch(k8sconfig.client, patch.url, patch.ops)
        if err:
            logit.error(f"Could not patch {msg_res}")
            return True

    # Delete the excess resources. Abort on first error.
    for data_d in plan.delete:
        msg_res = f"{data_d.meta.kind.upper()} {data_d.meta.namespace}/{data_d.meta.name}"
        print(f"Deleting {msg_res}")
        _, err = k8s.delete(k8sconfig.client, data_d.url, data_d.manifest)
        if err:
            logit.error(f"Could not patch {msg_res}")
            return True

    # All good.
    return False


def make_plan(cfg: Config) -> Tuple[DeploymentPlan, bool]:
    """Return the deployment plan.

    Returns:
        Deployment plan.

    """
    # Sanity check labels.
    if not all([valid_label(_) for _ in cfg.selectors.labels]):
        logit.error(f"Invalid labels: {cfg.selectors.labels}")
        return DeploymentPlan(tuple(), tuple(), tuple()), True

    try:
        # Create properly configured Requests session to talk to K8s API.
        k8sconfig, err = k8s.cluster_config(cfg.kubeconfig, cfg.kubecontext)
        assert not err

        # Convert "Selectors.kinds" to their canonical names.
        cfg = translate_resource_kinds(cfg, k8sconfig)

        # Load manifests from local files.
        local, _, err = manio.load_manifests(cfg.folder, cfg.selectors)
        assert not err

        # Download manifests from K8s.
        server, err = manio.download(cfg, k8sconfig)
        assert not err

        # Align non-plannable fields, like the ServiceAccount tokens.
        local_meta, err = manio.align_serviceaccount(local, server)
        assert not err

        # Create deployment plan.
        plan, err = compile_plan(cfg, k8sconfig, local_meta, server)
        assert not err and plan
    except AssertionError:
        return (DeploymentPlan(tuple(), tuple(), tuple()), True)

    # Print the plan and return.
    return (plan, False)


def get_resources(cfg: Config) -> bool:
    """Download all K8s manifests and merge them into local files."""
    # Sanity check labels.
    if not all([valid_label(_) for _ in cfg.selectors.labels]):
        logit.error(f"Invalid labels: {cfg.selectors.labels}")
        return True

    try:
        # Create properly configured Requests session to talk to K8s API.
        k8sconfig, err = k8s.cluster_config(cfg.kubeconfig, cfg.kubecontext)
        assert not err

        # Convert "Selectors.kinds" to their canonical names.
        cfg = translate_resource_kinds(cfg, k8sconfig)

        # Use a wildcard Selector to ensure `manio.load` will read _all_ local
        # manifests. This will allow `manio.sync` to modify the ones specified
        # by the `selector` argument only, delete all the local manifests and
        # then create the new ones. This logic will ensure we never have stale
        # manifests. Refer to `manio.save_files` for details and how
        # `manio.save` uses it.
        load_selectors = Selectors(kinds=k8sconfig.kinds, labels=[], namespaces=[])

        # Load manifests from local files.
        local_meta, local_path, err = manio.load_manifests(cfg.folder, load_selectors)
        assert not err
        del load_selectors

        # Download manifests from K8s.
        server, err = manio.download(cfg, k8sconfig)
        assert not err

        # Replace the server resources fetched from K8s' preferred endpoint with
        # the one from the endpoint referenced in the local manifest.
        server, err = match_api_version(k8sconfig, local_meta, server)
        assert not err

        # Sync the server manifests into the local manifests. All this happens in
        # memory and no files will be modified here - see `manio.save` in the next step.
        synced_manifests, err = manio.sync(local_path, server, cfg.selectors, cfg.groupby)
        assert not err

        # Write the new manifest files.
        err = manio.save(cfg.folder, synced_manifests, cfg.priorities)
        assert not err
    except AssertionError:
        return True

    # Success.
    return False
