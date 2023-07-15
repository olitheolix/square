"""Use Square as a library.

Run it from the parent directory like so:

  $ cd integration-test-cluster; ./start_cluster.sh; cd ..
  $ PYTHONPATH=`pwd` pipenv run python examples/as_library.py

"""
import os
import pathlib

import square
from square.dtypes import Config, GroupBy, Selectors


def main():
    # ----------------------------------------------------------------------
    #                                 Setup
    # ----------------------------------------------------------------------
    # Kubernetes credentials.
    kubeconfig, kubecontext = pathlib.Path(os.environ["KUBECONFIG"]), None

    # Optional: Set log level (0 = ERROR, 1 = WARNING, 2 = INFO, 3 = DEBUG).
    square.square.setup_logging(3)

    # Populate the `Config` structure. All main functions expect this.
    config = Config(
        kubeconfig=kubeconfig,
        kubecontext=kubecontext,

        # Store manifests in this folder.
        folder=pathlib.PosixPath('manifests'),

        # Organise the manifests in `folder` above in this order.
        groupby=GroupBy(label="app", order=["ns", "label", "kind"]),

        # Specify the resources to operate on. These ones model the demo setup in
        # the `../integration-test-cluster`.
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
    err = square.get(config)
    assert not err

    # Compute the plan and show it. It will not show any differences because we
    # planned against the resources we just downloaded.
    plan, err = square.plan(config)
    assert not err
    square.show_plan(plan)

    # Apply the plan - will not actually do anything in this case, because the
    # plan is empty.
    err = square.apply_plan(config, plan)
    assert not err


if __name__ == "__main__":
    main()
