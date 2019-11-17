"""Use Square as a library"""
import os
import pathlib

import square
from square.dtypes import GroupBy, Selectors


def main():
    # Define the necessary variables.
    kubeconfig = pathlib.Path(os.environ["KUBECONFIG"])
    kubectx = None
    folder = pathlib.PosixPath('manifests')
    selectors = Selectors(
        kinds=["Deployment", "Service", "Namespace"],
        labels={("app", "demoapp")},
        namespaces=["square-tests"],
    )
    groupby = GroupBy(label="app", order=["ns", "label", "kind"])

    # Optional: configures Square's logger.
    square.square.setup_logging(0)

    # Convenience.
    common_args = kubeconfig, kubectx, folder, selectors

    # Download manifests.
    _, err = square.get(*common_args, groupby)

    # Compute plan, then print it.
    plan, err = square.plan(*common_args)
    square.square.print_deltas(plan)

    # Apply the plan.
    _, err = square.apply(*common_args, plan, "yes")


if __name__ == "__main__":
    main()
