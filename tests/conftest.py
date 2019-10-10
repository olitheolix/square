import unittest.mock as mock

import pytest
import square.square


def pytest_configure(*args, **kwargs):
    """Pytest calls this hook on startup."""
    # Set log level to DEBUG for all unit tests.
    square.square.setup_logging(9)


@pytest.fixture
def kube_creds(request):
    with mock.patch.object(square.square, "cluster_config") as m:
        m.return_value = (("k8s_config", "k8s_client"), False)
        yield m
