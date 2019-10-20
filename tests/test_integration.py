import pathlib
import unittest.mock as mock

import pytest
import requests
import sh
import square.main
import square.manio as manio
import yaml


def kind_available():
    """Return True if KIND cluster is available."""
    try:
        resp = requests.get("http://localhost:10080/kubernetes-ready")
    except requests.ConnectionError:
        return False
    else:
        return resp.status_code == 200


@pytest.mark.skipif(not kind_available(), reason="No Minikube")
class TestMainGet:
    def setup_method(self):
        cur_path = pathlib.Path(__file__).parent.parent
        integration_test_manifest_path = cur_path / "integration-test-cluster"
        assert integration_test_manifest_path.exists()
        assert integration_test_manifest_path.is_dir()
        sh.kubectl(
            "--kubeconfig", "/tmp/kubeconfig-kind.yaml", "apply",
            "-f", str(integration_test_manifest_path)
        )

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
        for ns in ["default", "kube-system", "square-tests-1"]:
            assert (tmp_path / ns).exists()
            assert (tmp_path / ns).is_dir()

        # All our integration test resources have the `app=demoapp-1` label and
        # must thus exist in the folder "square-tests-1/demoapp-1".
        demoapp_1_path = tmp_path / "square-tests-1" / "demoapp-1"
        assert (demoapp_1_path).exists()
        assert (demoapp_1_path).is_dir()

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
            assert (demoapp_1_path / f"{kind}.yaml").exists()
            assert not (demoapp_1_path / f"{kind}.yaml").is_dir()

        # Un-namespaced resources must be in special "_global_" folder.
        for kind in ["clusterrole", "clusterrolebinding"]:
            assert (tmp_path / "_global_" / "demoapp-1" / f"{kind}.yaml").exists()
            assert not (tmp_path / "_global_" / "demoapp-1" / f"{kind}.yaml").is_dir()

    def test_main_get_single_namespace(self, tmp_path):
        """Sync individual resources in single namespace.

        This test will use `kubectl` to delete some resources.

        """
        # Convenience.
        man_path = tmp_path / "square-tests-1" / "demoapp-1"

        # Common command line arguments for GET command used in this test.
        common_args = (
            "--folder", str(tmp_path),
            "--groupby", "ns", "label=app", "kind",
            "-n", "square-tests-1",
            "--kubeconfig", "/tmp/kubeconfig-kind.yaml",
        )

        # Sync Deployments: must create "deployment.yaml".
        args = ("square.py", "get", "deploy", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()
        assert len(list(tmp_path.rglob("*.yaml"))) == 1
        assert (man_path / "deployment.yaml").exists()

        # Sync Ingresses: must add "ingress.yaml".
        args = ("square.py", "get", "ingress", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()
        assert len(list(tmp_path.rglob("*.yaml"))) == 2
        assert (man_path / "deployment.yaml").exists()
        assert (man_path / "ingress.yaml").exists()
        man_dpl = (man_path / "deployment.yaml").read_bytes()
        man_ing = (man_path / "ingress.yaml").read_bytes()

        # Delete the Deployment but sync only Ingresses: no change in files.
        sh.kubectl("--kubeconfig", "/tmp/kubeconfig-kind.yaml", "delete",
                   "deploy", "demoapp-1", "-n", "square-tests-1")
        args = ("square.py", "get", "ingress", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()
        assert len(list(tmp_path.rglob("*.yaml"))) == 2
        assert (man_path / "deployment.yaml").exists()
        assert (man_path / "ingress.yaml").exists()
        assert man_dpl == (man_path / "deployment.yaml").read_bytes()
        assert man_ing == (man_path / "ingress.yaml").read_bytes()

        # Sync Deployment: must remove the original "deployment.yaml"
        # because we have no deployments anymore.
        args = ("square.py", "get", "deploy", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()
        assert len(list(tmp_path.rglob("*.yaml"))) == 1
        assert (man_path / "ingress.yaml").exists()

    def test_main_get_both_namespaces(self, tmp_path):
        """Sync individual resources across two namespaces.

        This test will use `kubectl` to delete some resources.

        """
        def load_manifests(path):
            manifests = yaml.safe_load_all(open(path / "_other.yaml"))
            manifests = {manio.make_meta(_) for _ in manifests}
            manifests = {(_.kind, _.namespace, _.name) for _ in manifests}
            return manifests

        # Convenience.
        man_path = tmp_path

        # Common command line arguments for GET command used in this test.
        # Unlike in the previous test, we will store all manifests in a single
        # file (ie we omit the "--groupby" option).
        common_args = (
            "--folder", str(tmp_path),
            "--kubeconfig", "/tmp/kubeconfig-kind.yaml",
        )

        # Sync Deployments & Ingresses: must create catchall file "_other.yaml".
        args = ("square.py", "get", "deploy", "ingress",
                "-n", "square-tests-1", "square-tests-2",
                *common_args)

        with mock.patch("sys.argv", args):
            square.main.main()
        assert len(list(tmp_path.rglob("*.yaml"))) == 1
        assert (man_path / "_other.yaml").exists()

        # Ensure we got 4 manifests: one ingress and one deployment for each namespace.
        assert load_manifests(man_path) == {
            ('Deployment', 'square-tests-1', 'demoapp-1'),
            ('Deployment', 'square-tests-2', 'demoapp-1'),
            ('Ingress', 'square-tests-1', 'demoapp-1'),
            ('Ingress', 'square-tests-2', 'demoapp-1'),
        }

        # Delete the "demoapp-1" Ingress in one namespace then sync only those
        # from the other namespace: must not change the local manifests.
        sh.kubectl("--kubeconfig", "/tmp/kubeconfig-kind.yaml", "delete",
                   "ingress", "demoapp-1", "-n", "square-tests-1")
        args = ("square.py", "get", "ingress", "-n", "square-tests-2", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()
        assert load_manifests(man_path) == {
            ('Deployment', 'square-tests-1', 'demoapp-1'),
            ('Deployment', 'square-tests-2', 'demoapp-1'),
            ('Ingress', 'square-tests-1', 'demoapp-1'),
            ('Ingress', 'square-tests-2', 'demoapp-1'),
        }

        # Now delete the Deployments in both Namespaces and sync Ingresses,
        # also from both Namespaces: must only remove the ingress we deleted in
        # the previous step.
        sh.kubectl("--kubeconfig", "/tmp/kubeconfig-kind.yaml", "delete",
                   "deploy", "demoapp-1", "-n", "square-tests-1")
        sh.kubectl("--kubeconfig", "/tmp/kubeconfig-kind.yaml", "delete",
                   "deploy", "demoapp-1", "-n", "square-tests-2")
        args = ("square.py", "get", "ingress",
                "-n", "square-tests-1", "square-tests-2", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()
        assert load_manifests(man_path) == {
            ('Deployment', 'square-tests-1', 'demoapp-1'),
            ('Deployment', 'square-tests-2', 'demoapp-1'),
            ('Ingress', 'square-tests-2', 'demoapp-1'),
        }

        # Sync Deployments from both namespaces: must not leave any Deployments
        # because we deleted them in a previous step.
        args = ("square.py", "get", "deploy",
                "-n", "square-tests-1", "square-tests-2", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()
        assert load_manifests(man_path) == {
            ('Ingress', 'square-tests-2', 'demoapp-1'),
        }

        # Delete the last ingress and sync again: must delete "_other.yaml"
        # altogether because now we have no resources left anymore.
        sh.kubectl("--kubeconfig", "/tmp/kubeconfig-kind.yaml", "delete",
                   "ingress", "demoapp-1", "-n", "square-tests-2")
        args = ("square.py", "get", "ingress",
                "-n", "square-tests-1", "square-tests-2", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()
        assert len(list(tmp_path.rglob("*.yaml"))) == 0


class TestMainPlan:
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
