"""Import two clusters.

This example just shows how to asynchronously import two clusters.

"""
import asyncio
from pathlib import Path

import square
from square.dtypes import Config, GroupBy, Selectors


def make_config(kubeconfig: Path, kubecontext: str, folder: Path):
    folder = folder / kubecontext

    # Return a `Config` structure for the current cluster.
    return Config(
        kubeconfig=kubeconfig,
        kubecontext=kubecontext,

        # Store manifests in this folder.
        folder=folder,

        # Group the downloaded manifests by namespace, label and kind.
        groupby=GroupBy(label="app", order=["ns", "label", "kind"]),

        # Specify the resources types to operate on. These ones in particular
        # will work with the demo setup in the `../integration-test-cluster`.
        selectors=Selectors(
            kinds={"Deployment", "Service", "Namespace"},
            labels=[],
            namespaces=[],
        ),
    )


async def main():
    # Specify path to Kubernetes credentials.
    kubeconfig = Path("/tmp/kubeconfig-kind.yaml")

    # Optional: Set log level (0 = ERROR, 1 = WARNING, 2 = INFO, 3 = DEBUG).
    square.square.setup_logging(1)

    clusters = [
        (kubeconfig, "kind-kind", Path("clusters/kind-1")),
        (kubeconfig, "kind-kind", Path("clusters/kind-2")),
    ]

    # Populate the `Config` structure for Square.
    configs = [make_config(*args) for args in clusters]

    # Import resources and ensure all `square.get` calls returned without error.
    coroutines = [square.get(cfg) for cfg in configs]
    err = await asyncio.gather(*coroutines)
    assert not any(err)


if __name__ == "__main__":
    asyncio.run(main())
