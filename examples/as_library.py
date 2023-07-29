"""Use Square as a library.

This example leverages the Square library to import manifests from a server,
create a plan and then apply it.

Start the KinD cluster, then run this script from the parent directory like so:

  $ cd integration-test-cluster; ./start_cluster.sh; cd ..
  $ PYTHONPATH=`pwd` pipenv run python examples/as_library.py

"""
import os
from pathlib import Path

import square
from square.dtypes import Config, GroupBy, Selectors


def main():
    # ----------------------------------------------------------------------
    #                                 Setup
    # ----------------------------------------------------------------------
    # Kubernetes credentials.
    kubeconfig, kubecontext = Path(os.environ["KUBECONFIG"]), None

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
    err = square.get(config)
    assert not err

    # Create the plan and show it.
    # NOTE: the plan will be clean since we only just downloaded the manifests.
    plan, err = square.plan(config)
    assert not err
    square.show_plan(plan)

    # Apply the plan.
    # NOTE: will do nothing because the plan was clean.
    err = square.apply_plan(config, plan)
    assert not err


if __name__ == "__main__":
    main()
