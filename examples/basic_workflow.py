"""Use Square as a library in your own code.

This example will download some manifests from the KinD integration test
server, create a plan and then apply it.

"""
import asyncio
from pathlib import Path

import square
from square.dtypes import Config, GroupBy, Selectors


async def main():
    # Specify path to Kubernetes credentials.
    kubeconfig, kubecontext = Path("/tmp/kubeconfig-kind.yaml"), None

    # ----------------------------------------------------------------------
    #                                 Setup
    # ----------------------------------------------------------------------
    # Optional: Set log level (0 = ERROR, 1 = WARNING, 2 = INFO, 3 = DEBUG).
    square.square.setup_logging(1)

    # Populate the `Config` structure. All primary functions, ie `get`, `plan`
    # and `apply` need one.
    config = Config(
        kubeconfig=kubeconfig,
        kubecontext=kubecontext,

        # Store manifests in this folder.
        folder=Path('manifests'),

        # Group the downloaded manifests by namespace, label and kind.
        groupby=GroupBy(label="app", order=["ns", "label", "kind"]),

        # Specify the resources types to operate on. These ones in particular
        # will work with the demo setup in the `../integration-test-cluster`.
        selectors=Selectors(
            kinds={"Deployment", "Service", "Namespace"},
            labels=["app=demoapp-1"],
            namespaces=["square-tests-1", "square-tests-2"],
        ),
    )

    # ----------------------------------------------------------------------
    #          Import resources, create a plan and then apply it
    # ----------------------------------------------------------------------
    # Download the manifests into the `folder` defined earlier.
    err = await square.get(config)
    assert not err

    # Create the plan and show it.
    # NOTE: the plan will be clean since we only just downloaded the manifests.
    plan, err = await square.plan(config)
    assert not err
    square.show_plan(plan)

    # Apply the plan.
    # NOTE: will do nothing because the plan was clean.
    err = await square.apply_plan(config, plan)
    assert not err


if __name__ == "__main__":
    asyncio.run(main())
