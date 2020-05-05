import pathlib
import unittest.mock as mock

import pytest
import square.dtypes
import square.square

from .test_helpers import k8s_apis


def pytest_configure(*args, **kwargs):
    """Pytest calls this hook on startup."""
    # Set log level to DEBUG for all unit tests.
    square.square.setup_logging(9)

    if pathlib.Path(".square.yaml").exists():
        print("\n--- Found `.square.yaml` in root folder. "
              "The tests cannot tolerate that. ABORT ---\n")
        assert False


@pytest.fixture
def kube_creds(request, k8sconfig):
    with mock.patch.object(square.k8s, "cluster_config") as m:
        m.return_value = (k8sconfig, False)
        yield k8sconfig


@pytest.fixture
def k8sconfig():
    # Return a valid K8sConfig with a subsection of API endpoints available in
    # K8s v1.15.
    cfg = square.dtypes.K8sConfig(version="1.15", client="k8s_client")

    # The set of API endpoints we can use in the tests.
    cfg.apis.clear()
    cfg.apis.update(k8s_apis(cfg))

    # Manually insert common short spellings.
    cfg.short2kind["deployment"] = "Deployment"
    cfg.short2kind["service"] = "Service"
    cfg.short2kind["svc"] = "Service"
    cfg.short2kind["secret"] = "Secret"

    # The set of canonical K8s resources we support.
    cfg.kinds.update({_ for _ in cfg.short2kind.values()})

    # Pass the fixture to the test.
    yield cfg


@pytest.fixture
def config(k8sconfig, tmp_path):
    """Return a valid and fully populated `Config` structure.

    The data in the structure matches `tests/support/config.yaml` except for
    the `kubeconfig` file. That one is different and points to an actually
    (dummy) file in a temporary folder for this test.

    """
    # Load the sample configuration.
    cfg, err = square.square.load_config("tests/support/config.yaml")
    assert not err

    # Point the folder and kubeconfig to temporary versions.
    cfg.folder = tmp_path
    cfg.kubeconfig = (tmp_path / "kubeconf")

    # Ensure the dummy kubeconfig file exists.
    cfg.kubeconfig.write_text("")

    yield cfg
