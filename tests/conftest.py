import asyncio
import unittest.mock as mock
from pathlib import Path
from typing import Generator

import httpx
import pytest

import square.callbacks
import square.cfgfile
import square.k8s
import square.square
from square.dtypes import Config, K8sConfig

from .test_helpers import k8s_apis


def pytest_configure(*args, **kwargs):
    """Pytest calls this hook on startup."""
    # Set log level to DEBUG for all unit tests.
    square.square.setup_logging(9)

    if Path(".square.yaml").exists():
        print("\n--- Found `.square.yaml` in root folder. "
              "The tests cannot tolerate that. ABORT ---\n")
        assert False


@pytest.fixture
def kube_creds(request, k8sconfig) -> Generator[K8sConfig, None, None]:
    with mock.patch.object(square.k8s, "cluster_config") as m:
        m.return_value = (k8sconfig, False)
        yield k8sconfig


@pytest.fixture
def k8sconfig():
    # Return a valid K8sConfig with a subsection of API endpoints available in
    # Kubernetes v1.25.
    cadata = Path("tests/support/client.crt").read_text()
    cfg = K8sConfig(version="1.25", client=httpx.AsyncClient(), cadata=cadata)

    # The set of API endpoints we can use in the tests.
    cfg.apis.clear()
    cfg.apis.update(k8s_apis(cfg))

    # Manually insert common short spellings.
    cfg.short2kind["deployment"] = "Deployment"
    cfg.short2kind["service"] = "Service"
    cfg.short2kind["svc"] = "Service"
    cfg.short2kind["secret"] = "Secret"
    cfg.short2kind["ns"] = "Namespace"
    cfg.short2kind["namespace"] = "Namespace"
    cfg.short2kind["hpa"] = "HorizontalPodAutoscaler"
    cfg.short2kind["cm"] = "ConfigMap"

    # The set of canonical K8s resources we support.
    cfg.kinds.update({_ for _ in cfg.short2kind.values()})

    # Short-circuit the `async.sleep` function.
    with mock.patch.object(asyncio, "sleep"):
        yield cfg


@pytest.fixture
def config(k8sconfig, tmp_path) -> Generator[Config, None, None]:
    """Return a valid and fully populated `Config` structure.

    The data in the structure matches `tests/support/config.yaml` except for
    the `kubeconfig` file. That one is different and points to an actual
    (dummy) file in a temporary folder for this test.

    """
    # Load the sample configuration.
    cfg, err = square.cfgfile.load(Path("tests/support/config.yaml"))
    assert not err

    # Point the folder and kubeconfig to temporary versions.
    cfg.folder = tmp_path
    cfg.kubeconfig = (tmp_path / "kubeconf")

    # Ensure the dummy kubeconfig file exists.
    cfg.kubeconfig.write_text("")

    yield cfg
