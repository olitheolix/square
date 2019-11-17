"""Use Square as a library"""
import os
import pathlib

import square
from square.dtypes import GroupBy, Selectors


def main():
    # ----------------------------------------------------------------------
    #                                 Setup
    # ----------------------------------------------------------------------
    # Kubernetes credentials.
    kubeconfig, kube_ctx = pathlib.Path(os.environ["KUBECONFIG"]), None

    # Specify the resources to operate on. These ones model the demo setup in
    # the `../integration-test-cluster`.
    selectors = Selectors(
        kinds=["Deployment", "Service", "Namespace"],
        labels={("app", "demoapp-1")},
        namespaces=["square-tests-1", "square-tests-2"],
    )

    # Where to store the manifests and how to organise them inside that folder.
    folder = pathlib.PosixPath('manifests')
    groupby = GroupBy(label="app", order=["ns", "label", "kind"])

    # Optional: Set log level (0 = ERROR, 1 = WARNING, 2 = INFO, 3 = DEBUG).
    square.square.setup_logging(3)

    # ----------------------------------------------------------------------
    #          Import resources, create a plan and then apply it
    # ----------------------------------------------------------------------
    # Download the manifests into the `folder` defined earlier.
    _, err = square.get(kubeconfig, kube_ctx, folder, selectors, groupby)
    assert not err

    # Compute the plan and show it. It will not show any differences because we
    # planned against the resources we just downloaded.
    plan, err = square.plan(kubeconfig, kube_ctx, folder, selectors)
    assert not err
    square.show_plan(plan)

    # Apply the plan - will not actually do anything in this case, because the
    # plan is empty.
    _, err = square.apply_plan(kubeconfig, kube_ctx, plan)
    assert not err


if __name__ == "__main__":
    main()
