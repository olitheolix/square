"""Use custom callbacks to strip manifests or modify them before computing patches.

This example will first download some manifests from the KinD integration test
cluster and verify that the plan is clean.

Then it will install a new patch callback that adds a label but only to
DEPLOYMENTS and compute the same plan again. This time, the plan will contain
some patches.

"""
import asyncio
from pathlib import Path
from typing import Tuple

import square
from square.dtypes import Config, Selectors


# Install a `strip` callback that corrupts `MetaManifest` info.
def strip_callback(cfg: Config, manifest: dict):
    """Remove the `status` field from all DEPLOYMENT manifests."""
    if manifest["kind"] == "Deployment":
        manifest.pop("status", None)
    return manifest


def patch_callback(cfg: Config,
                   local_manifest: dict,
                   server_manifest: dict) -> Tuple[dict, dict]:
    """Modify the local manifest before Square compiles patches."""
    # Make no modifications if the manifest is not a DEPLOYMENT.
    if local_manifest["kind"] != "Deployment":
        return local_manifest, server_manifest

    # Ensure the DEPLOYMENT has a label `foo=bar`.
    if "labels" not in local_manifest["metadata"]:
        local_manifest["metadata"]["labels"] = {}
    local_manifest["metadata"]["labels"]["foo"] = "bar"

    # Do not patch the replica count of a deployment. This is a typical use
    # case for this kind of callback since the HPA may have increased the
    # number of replicas in the cluster and we probably do not want to force it
    # back to the value specified in our local manifest. We can easily avoid
    # this by showing Square a local manifest that has the exact same amount of
    # replicas as that on the cluster.
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
        folder=Path('manifests'),
        selectors=Selectors(
            kinds={"Deployment", "Service", "Namespace"},
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
    assert plan.create == plan.patch == plan.delete == []

    # Install our custom patch callback to insert a new label to all
    # DEPLOYMENTS. The plan must now contain patches.
    config.patch_callback = patch_callback
    plan, err = await square.plan(config)
    assert not err
    assert plan.create == plan.delete == []
    assert len(plan.patch) > 0


if __name__ == "__main__":
    asyncio.run(main())
