import unittest.mock as mock

import pytest
import square.dtypes
import square.square

from .test_helpers import k8s_apis


def pytest_configure(*args, **kwargs):
    """Pytest calls this hook on startup."""
    # Set log level to DEBUG for all unit tests.
    square.square.setup_logging(9)


@pytest.fixture
def kube_creds(request):
    with mock.patch.object(square.k8s, "cluster_config") as m:
        m.return_value = ("k8s_config", "k8s_client", False)
        yield m


@pytest.fixture
def k8sconfig():
    # Return a valid K8sConfig with a subsection of API endpoints available in
    # K8s v1.15.
    cfg = square.dtypes.K8sConfig(version="1.15")

    # Update the API endpoints.
    cfg.apis.clear()
    cfg.apis.update(k8s_apis(cfg))

    # Pass the fixture to the test.
    yield cfg
