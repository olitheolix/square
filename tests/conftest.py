import unittest.mock as mock

import pytest
import square.dtypes
import square.square
from square.dtypes import SUPPORTED_KINDS, Config, GroupBy, Selectors

from .test_helpers import k8s_apis


def pytest_configure(*args, **kwargs):
    """Pytest calls this hook on startup."""
    # Set log level to DEBUG for all unit tests.
    square.square.setup_logging(9)


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
    kubeconf = (tmp_path / "kubeconf")
    kubeconf.write_text("")

    cfg = Config(
        folder=tmp_path,
        kubeconfig=kubeconf,
        kube_ctx=None,
        selectors=Selectors(
            kinds=set(k8sconfig.kinds),
            namespaces=['default'],
            labels={("app", "demo")},
        ),
        groupby=GroupBy("", tuple()),
        priorities=SUPPORTED_KINDS
    )
    yield cfg
