import pathlib
import time
import unittest.mock as mock

import pytest
import requests
import sh
import square.main
import square.manio as manio
import yaml
from square.dtypes import (
    SUPPORTED_KINDS, Filepath, GroupBy, K8sResource, Selectors,
)

# Bake a `kubectl` command that uses the kubeconfig file for the KIND cluster.
# We need to wrap this into a try/except block because CircleCI does not have
# `kubectl` installed and cannot run the integration tests anyway.
try:
    kubectl = sh.kubectl.bake("--kubeconfig", "/tmp/kubeconfig-kind.yaml")
except sh.CommandNotFound:
    kubectl = None


def kind_available():
    """Return True if KIND cluster is available."""
    try:
        resp = requests.get("http://localhost:10080/kubernetes-ready")
    except requests.ConnectionError:
        return False
    else:
        return resp.status_code == 200


@pytest.mark.skipif(not kind_available(), reason="No Minikube")
class TestEndpoints:
    def setup_method(self):
        cur_path = pathlib.Path(__file__).parent.parent
        integration_test_manifest_path = cur_path / "integration-test-cluster"
        assert integration_test_manifest_path.exists()
        assert integration_test_manifest_path.is_dir()
        kubectl("apply", "-f", str(integration_test_manifest_path))

    def test_compile_api_endpoints(self):
        """Ask for all endpoints and perform some sanity checks."""
        square.square.setup_logging(1)
        kubeconfig = Filepath("/tmp/kubeconfig-kind.yaml")
        apis, err = square.k8s.compile_api_endpoints(kubeconfig, None)
        assert not err

        (config, client), err = square.k8s.cluster_config(kubeconfig, None)
        assert not err and config and client

        # Sanity check.
        kinds = {
            # Standard resources that a v1.13 Kubernetes cluster always has.
            ('ClusterRole', 'rbac.authorization.k8s.io/v1'),
            ('ClusterRole', 'rbac.authorization.k8s.io/v1beta1'),
            ('ConfigMap', 'v1'),
            ('DaemonSet', 'apps/v1'),
            ('DaemonSet', 'apps/v1beta2'),
            ('DaemonSet', 'extensions/v1beta1'),
            ('Deployment', 'apps/v1'),
            ('Deployment', 'apps/v1beta1'),
            ('Deployment', 'apps/v1beta2'),
            ('Deployment', 'extensions/v1beta1'),
            ('Pod', 'v1'),
            ('Service', 'v1'),
            ('ServiceAccount', 'v1'),

            # Our CustomCRD.
            ("DemoCRD", "mycrd.com/v1"),
        }
        assert kinds.issubset(set(apis.keys()))

        # Verify some standard resource types.
        assert apis[("Namespace", "v1")] == K8sResource(
            apiVersion="v1",
            kind="Namespace",
            name="namespaces",
            namespaced=False,
            url=f"{config.url}/api/v1/namespaces",
        )
        assert apis[("Pod", "v1")] == K8sResource(
            apiVersion="v1",
            kind="Pod",
            name="pods",
            namespaced=True,
            url=f"{config.url}/api/v1/pods",
        )
        assert apis[("Deployment", "apps/v1")] == K8sResource(
            apiVersion="apps/v1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1/deployments",
        )

        # Verify our CRD.
        assert apis[("DemoCRD", "mycrd.com/v1")] == K8sResource(
            apiVersion="mycrd.com/v1",
            kind="DemoCRD",
            name="democrds",
            namespaced=True,
            url=f"{config.url}/apis/mycrd.com/v1/democrds",
        )


@pytest.mark.skipif(not kind_available(), reason="No Minikube")
class TestMainGet:
    def setup_method(self):
        cur_path = pathlib.Path(__file__).parent.parent
        integration_test_manifest_path = cur_path / "integration-test-cluster"
        assert integration_test_manifest_path.exists()
        assert integration_test_manifest_path.is_dir()
        kubectl("apply", "-f", str(integration_test_manifest_path))

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
        kubectl("delete", "deploy", "demoapp-1", "-n", "square-tests-1")
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
            # Load all manifests and return just the metadata.
            manifests = yaml.safe_load_all(open(path / "_other.yaml"))
            manifests = {manio.make_meta(_) for _ in manifests}
            manifests = {(_.kind, _.namespace, _.name) for _ in manifests}
            return manifests

        # Convenience: manifest path.
        man_path = tmp_path

        # Common command line arguments for GET command used in this test.
        # Unlike in the previous test, we will store all manifests in a single
        # file (ie we omit the "--groupby" option).
        common_args = (
            "--folder", str(tmp_path),
            "--kubeconfig", "/tmp/kubeconfig-kind.yaml",
        )

        # ---------------------------------------------------------------------
        # Sync Deployments & Ingresses: must create catchall file "_other.yaml".
        # ---------------------------------------------------------------------
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

        # ---------------------------------------------------------------------
        # Delete the "demoapp-1" Ingress in one namespace then sync only those
        # from the other namespace: must not change the local manifests.
        # ---------------------------------------------------------------------
        kubectl("delete", "ingress", "demoapp-1", "-n", "square-tests-1")
        args = ("square.py", "get", "ingress", "-n", "square-tests-2", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()
        assert load_manifests(man_path) == {
            ('Deployment', 'square-tests-1', 'demoapp-1'),
            ('Deployment', 'square-tests-2', 'demoapp-1'),
            ('Ingress', 'square-tests-1', 'demoapp-1'),
            ('Ingress', 'square-tests-2', 'demoapp-1'),
        }

        # ---------------------------------------------------------------------
        # Now delete the Deployments in both Namespaces and sync Ingresses,
        # also from both Namespaces: must only remove the ingress we deleted in
        # the previous step.
        # ---------------------------------------------------------------------
        kubectl("delete", "deploy", "demoapp-1", "-n", "square-tests-1")
        kubectl("delete", "deploy", "demoapp-1", "-n", "square-tests-2")
        args = ("square.py", "get", "ingress",
                "-n", "square-tests-1", "square-tests-2", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()
        assert load_manifests(man_path) == {
            ('Deployment', 'square-tests-1', 'demoapp-1'),
            ('Deployment', 'square-tests-2', 'demoapp-1'),
            ('Ingress', 'square-tests-2', 'demoapp-1'),
        }

        # ---------------------------------------------------------------------
        # Sync Deployments from both namespaces: must not leave any Deployments
        # because we deleted them in a previous step.
        # ---------------------------------------------------------------------
        args = ("square.py", "get", "deploy",
                "-n", "square-tests-1", "square-tests-2", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()
        assert load_manifests(man_path) == {
            ('Ingress', 'square-tests-2', 'demoapp-1'),
        }

        # ---------------------------------------------------------------------
        # Delete the last ingress and sync again: must delete "_other.yaml"
        # altogether because now we have no resources left anymore.
        # ---------------------------------------------------------------------
        kubectl("delete", "ingress", "demoapp-1", "-n", "square-tests-2")
        args = ("square.py", "get", "ingress",
                "-n", "square-tests-1", "square-tests-2", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()
        assert len(list(tmp_path.rglob("*.yaml"))) == 0


@pytest.mark.skipif(not kind_available(), reason="No Minikube")
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

    def test_workflow(self, tmp_path):
        """Delete and restore full namespace with Square.

        We will use `kubectl` to create a new namespace and populate it with
        resource. Then we will use Square to backup it up, delete it and
        finally restore it again.

        """
        # Only show INFO and above or otherwise this test will produce a
        # humongous amount of logs from all the K8s calls.
        square.square.setup_logging(2)

        # Convenience.
        apply_plan = square.square.apply_plan
        make_plan = square.square.make_plan
        get_resources = square.square.get_resources

        # Fixtures.
        selectors = Selectors(SUPPORTED_KINDS, ["test-workflow"], None)
        groupby = GroupBy(label="app", order=[])
        backup_folder = tmp_path / "backup"

        # ---------------------------------------------------------------------
        # Deploy a new namespace with only a few resources (no deployments
        # among them because it takes too long to delete those namespaces).
        # ---------------------------------------------------------------------
        kubeconfig = Filepath("/tmp/kubeconfig-kind.yaml")
        sh.kubectl("apply", "--kubeconfig", kubeconfig,
                   "-f", "tests/support/k8s-test-resources.yaml")

        # ---------------------------------------------------------------------
        # Create a plan for "square-tests". The plan must delete all resources
        # because we have not downloaded any manifests yet.
        # ---------------------------------------------------------------------
        plan_1, err = square.square.make_plan(kubeconfig, None, tmp_path, selectors)
        assert not err
        assert plan_1.create == plan_1.patch == [] and len(plan_1.delete) > 0

        # ---------------------------------------------------------------------
        # Backup all resources. A plan against that backup must be empty.
        # ---------------------------------------------------------------------
        assert not (backup_folder / "_other.yaml").exists()
        _, err = get_resources(kubeconfig, None, backup_folder, selectors, groupby)
        assert not err and (backup_folder / "_other.yaml").exists()

        plan_2, err = make_plan(kubeconfig, None, backup_folder, selectors)
        assert not err
        assert plan_2.create == plan_2.patch == plan_2.delete == []

        # ---------------------------------------------------------------------
        # Apply the first plan to delete all resources including the namespace.
        # ---------------------------------------------------------------------
        _, err = apply_plan(kubeconfig, None, plan_1)
        assert not err

        # ---------------------------------------------------------------------
        # Wait until K8s has deleted the namespace.
        # ---------------------------------------------------------------------
        while True:
            time.sleep(1)
            try:
                sh.kubectl("get", "ns", "test-workflow", "--kubeconfig", kubeconfig)
            except sh.ErrorReturnCode_1:
                break

        # ---------------------------------------------------------------------
        # Use backup manifests to restore the namespace.
        # ---------------------------------------------------------------------
        plan_3, err = square.square.make_plan(kubeconfig, None, backup_folder, selectors)
        assert not err
        assert plan_3.patch == plan_3.delete == [] and len(plan_3.create) > 0

        # Apply the new plan.
        _, err = apply_plan(kubeconfig, None, plan_3)
        assert not err

        plan_4, err = square.square.make_plan(kubeconfig, None, backup_folder, selectors)
        assert not err
        assert plan_4.create == plan_4.patch == plan_4.delete == []
