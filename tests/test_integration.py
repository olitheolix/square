import unittest.mock as mock

import pytest
import requests
import square.main


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
        # Command line arguments: get all manifests and group them by namespace,
        # "app" label and resource kind.
        args = (
            "square.py", "get", "all",
            "--folder", str(tmp_path),
            "--groupby", "ns", "label=app", "kind",
            "--kubeconfig", "/tmp/kubeconfig-kind.yaml",
        )

        # Temporary folder must be initially empty. After we pulled all
        # resources, it must contain some files.
        assert len(list(tmp_path.rglob("*.yaml"))) == 0
        with mock.patch("sys.argv", args):
            square.main.main()

        # The integration test cluster has these namespaces, which means Square
        # must have created equivalent folders.
        for ns in ["default", "kube-system", "square-tests"]:
            assert (tmp_path / ns).exists()
            assert (tmp_path / ns).is_dir()

        # All our integration test resources have the `app=demoapp` label and
        # must thus exist in the folder "square-tests/demoapp".
        assert (tmp_path / "square-tests" / "demoapp").exists()
        assert (tmp_path / "square-tests" / "demoapp").is_dir()

        # The "square-tests" namespace must have these manifests.
        kinds = [
            "configmap",
            "cronjob",
            "daemonset",
            "deployment",
            "horizontalpodautoscaler",
            "ingress",
            "namespace",
            "persistentvolumeclaim",
            "role",
            "rolebinding",
            "secret",
            "service",
            "serviceaccount",
            "statefulset",
        ]
        for kind in kinds:
            assert (tmp_path / "square-tests" / "demoapp" / f"{kind}.yaml").exists()
            assert not (tmp_path / "square-tests" / "demoapp" / f"{kind}.yaml").is_dir()

        # Un-namespaced resources must be in special "_global_" folder.
        for kind in ["clusterrole", "clusterrolebinding"]:
            assert (tmp_path / "demoapp" / f"{kind}.yaml").exists()
            assert not (tmp_path / "demoapp" / f"{kind}.yaml").is_dir()

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
            square.main.main()
