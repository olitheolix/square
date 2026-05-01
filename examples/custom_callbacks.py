"""Use custom callbacks to strip manifests or modify them before computing patches.

The example will install a new patch callback to add a label before Square
compares the manifests and derives patches. Consequently, the plan will contain
patches to add the new label.

"""

import asyncio
from pathlib import Path
from typing import Tuple

import square
import square.manio
from square.dtypes import Config, Selectors, GroupBy


# Install a `strip` callback to remove the `status` field from all DEPLOYMENT manifests.
def strip_callback(cfg: Config, manifest: dict):
    """Remove the `.status` fields from all DEPLOYMENT manifests.

    This is easier to achieve with the standard filters but this is just an
    example of how to use the `strip_callback` to implement custom filter
    logic.

    """
    if manifest["kind"] == "Deployment":
        manifest.pop("status", None)

    # Optional: apply the JSON path filters.
    manifest = square.manio.strip_single_manifest(cfg, manifest)
    return manifest


def patch_callback(
    cfg: Config, local_manifest: dict, server_manifest: dict
) -> Tuple[dict, dict]:
    """Modify the local manifest before Square compiles patches."""
    # Do nothing unless it is a DEPLOYMENT.
    if local_manifest["kind"] != "Deployment":
        return local_manifest, server_manifest

    # Manually insert the label `foo=bar`.
    if "labels" not in local_manifest["metadata"]:
        local_manifest["metadata"]["labels"] = {}
    local_manifest["metadata"]["labels"]["foo"] = "bar"

    # Do not patch the replica count of a deployment. This is a typical use
    # case for this kind of callback since the HPA may have increased the
    # number of replicas in the cluster and we probably do not want to
    # overwrite it. To avoid this, we can simply set the number of replicas in
    # the local manifest to be the same as that on the cluster. This way,
    # Square will not detect any difference in the number of replicas and will
    # not generate a patch.
    #
    # NOTE: this change happens in memory only and never propagates back to the
    # local manifest stored on disk.
    local_manifest["spec"]["replicas"] = server_manifest["spec"]["replicas"]
    return local_manifest, server_manifest


async def main():
    # ----------------------------------------------------------------------
    #                                 Setup
    # ----------------------------------------------------------------------
    # Optional: Set log level (0 = ERROR, 1 = WARNING, 2 = INFO, 3 = DEBUG).
    square.square.setup_logging(1)

    # Populate the `Config` structure. All primary functions, ie `get`, `plan`
    # and `apply` need one.
    config = Config(
        kubeconfig=Path("/tmp/kubeconfig-kind.yaml"),
        folder=Path("manifests"),
        groupby=GroupBy(label="app", order=["ns", "label", "kind"]),
        selectors=Selectors(
            kinds={"deployments.apps", "services.v1", "namespace"},
        ),
        strip_callback=strip_callback,
    )

    # ----------------------------------------------------------------------
    #          Import resources, create a plan and then apply it
    # ----------------------------------------------------------------------
    # Download the manifests into the `folder` defined earlier.
    err = await square.get(config)
    assert not err

    # Create the plan and verify that it is clean.
    plan, err = await square.plan(config)
    assert not err
    assert plan.create == plan.patch == plan.delete == [], plan

    # Install our custom patch callback to insert a new label to all
    # DEPLOYMENTS. The plan must now contain patches.
    config.patch_callback = patch_callback
    plan, err = await square.plan(config)
    assert not err
    assert plan.create == plan.delete == []
    assert len(plan.patch) > 0


if __name__ == "__main__":
    asyncio.run(main())
