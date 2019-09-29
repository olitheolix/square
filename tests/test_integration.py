import unittest.mock as mock

import pytest
import requests
import square.square


def kind_available():
    """Return True if KIND cluster is available."""
    try:
        resp = requests.get("http://localhost:10080/kubernetes-ready")
    except requests.ConnectionError:
        return False
    else:
        return resp.status_code == 200


@pytest.mark.skipif(not kind_available(), reason="No Minikube")
class TestMain:
    def test_main_get(self, tmp_path):
        """GET all cluster resources."""
        # Command line arguments.
        args = (
            "square.py", "get", "all",
            "--folder", str(tmp_path),
            "--kubeconfig", "/tmp/kubeconfig-kind.yaml",
        )

        # Temporary folder must be initially empty. After we pulled all
        # resources, it must contain some files.
        assert len(list(tmp_path.rglob("*.yaml"))) == 0
        with mock.patch("sys.argv", args):
            square.square.main()
        assert len(list(tmp_path.rglob("*.yaml"))) > 0

    def test_main_plan(self, tmp_path):
        """PLAN all cluster resources."""
        # Command line arguments.
        args = (
            "square.py", "plan", "all",
            "--folder", str(tmp_path),
            "--kubeconfig", "/tmp/kubeconfig-kind.yaml",
        )

        # Merely verify that the program does not break.
        with mock.patch("sys.argv", args):
            square.square.main()
