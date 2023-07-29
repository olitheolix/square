import copy
import time
import unittest.mock as mock
from pathlib import Path
from typing import Dict, cast

import httpx
import pytest
import sh
import yaml

import square.k8s
import square.main
import square.manio as manio
from square.dtypes import Config, GroupBy, K8sConfig, Selectors

from .test_helpers import kind_available, make_manifest

# Bake a `kubectl` command that uses the kubeconfig file for the KIND cluster.
# We need to wrap this into a try/except block because CircleCI does not have
# `kubectl` installed and cannot run the integration tests anyway.
try:
    kubectl = sh.kubectl.bake("--kubeconfig", "/tmp/kubeconfig-kind.yaml")  # type: ignore
except sh.CommandNotFound:
    kubectl = None


@pytest.mark.skipif(not kind_available(), reason="No Integration Test Cluster")
class TestBasic:
    def test_cluster_config(self):
        """Basic success/failure test for K8s configuration."""
        # Only show INFO and above or otherwise this test will produce a
        # humongous amount of logs from all the K8s calls.
        square.square.setup_logging(2)

        # Fixtures.
        fun = square.k8s.cluster_config
        fname = Path("/tmp/kubeconfig-kind.yaml")

        # Must produce a valid K8s configuration.
        cfg, err = fun(fname, None)
        assert not err and isinstance(cfg, K8sConfig)

        # Must not return a Kubernetes configuration if we could not create a
        # HttpX client.
        with mock.patch.object(square.k8s, "create_httpx_client") as m_client:
            m_client.return_value = (httpx.Client(), True)
            assert fun(fname, None) == (K8sConfig(), True)


@pytest.mark.skipif(not kind_available(), reason="No Integration Test Cluster")
class TestMainGet:
    def setup_method(self):
        cur_path = Path(__file__).parent.parent
        integration_test_manifest_path = cur_path / "integration-test-cluster"
        assert integration_test_manifest_path.exists()
        assert integration_test_manifest_path.is_dir()
        assert kubectl is not None
        kubectl("apply", "-f", str(integration_test_manifest_path))

    def test_main_get(self, tmp_path):
        """GET all cluster resources."""
        # The "square-tests" namespace must have these manifests.
        non_namespaced = [
            "clusterrole",
            "clusterrolebinding",
        ]
        namespaced = [
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

        # Command line arguments: get all manifests and group them by namespace,
        # "app" label and resource kind.
        args = (
            "square.py", "get", *namespaced, *non_namespaced,
            "--folder", str(tmp_path),
            "--groupby", "ns", "label=app", "kind",
            "--kubeconfig", "/tmp/kubeconfig-kind.yaml",
            "--labels",         # Clear default labels from `config.yaml`.
            "--namespaces",     # Clear default namespaces from `config.yaml`.
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

        for kind in namespaced:
            assert (demoapp_1_path / f"{kind}.yaml").exists()
            assert not (demoapp_1_path / f"{kind}.yaml").is_dir()

        # Un-namespaced resources must be in special "_global_" folder.
        for kind in non_namespaced:
            assert (tmp_path / "_global_" / "demoapp-1" / f"{kind}.yaml").exists()
            assert not (tmp_path / "_global_" / "demoapp-1" / f"{kind}.yaml").is_dir()

    def test_main_get_single_namespace(self, tmp_path):
        """Sync individual resources in single namespace.

        This test will use `kubectl` to delete some resources.

        """
        assert kubectl is not None

        # Convenience.
        man_path = tmp_path / "square-tests-1" / "demoapp-1"

        # Common command line arguments for GET command used in this test.
        common_args = (
            "--folder", str(tmp_path),
            "--groupby", "ns", "label=app", "kind",
            "-n", "square-tests-1",
            "--kubeconfig", "/tmp/kubeconfig-kind.yaml",
            "--labels",
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
        assert kubectl is not None

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
            "--labels", "--groupby"
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

    def test_nonpreferred_api(self, tmp_path):
        """Sync `autoscaling/v1` and `autoscaling/v2beta` at the same time.

        This test is designed to verify that Square will interrogate the
        correct K8s endpoint versions to download the manifest.

        """
        # Only show INFO and above or otherwise this test will produce a
        # humongous amount of useless logs from all the K8s calls.
        square.square.setup_logging(2)

        config = Config(
            folder=tmp_path,
            groupby=GroupBy(label="app", order=[]),
            kubecontext=None,
            kubeconfig=Path("/tmp/kubeconfig-kind.yaml"),
            selectors=Selectors(
                kinds={"HorizontalPodAutoscaler"},
                namespaces=["test-hpa"],
                labels=[],
            ),
        )

        # Copy the manifest with the namespace and the two HPAs to the temporary path.
        manifests = list(yaml.safe_load_all(open("tests/support/k8s-test-hpa.yaml")))
        man_path = tmp_path / "manifest.yaml"
        man_path.write_text(yaml.dump_all(manifests))
        assert len(manifests) == 3

        # ---------------------------------------------------------------------
        # Deploy two HPAs into the same namespace. One HPA will use
        # `autoscaling/v1` whereas the other uses `autoscaling/v2beta2`.
        # ---------------------------------------------------------------------
        sh.kubectl("apply", "--kubeconfig", config.kubeconfig,  # type: ignore
                   "-f", str(man_path))

        # ---------------------------------------------------------------------
        # Sync all manifests. This must do nothing. In particular, it must not
        # change the `apiVersion` of either HPA.
        # ---------------------------------------------------------------------
        assert not square.square.get_resources(config)
        assert list(yaml.safe_load_all(man_path.read_text())) == manifests


@pytest.mark.skipif(not kind_available(), reason="No Integration Test Cluster")
class TestKindName:
    def test_api_plan_kind_name_resource(self, tmp_path):
        """Plan a ConfigMap specified by its `configmap/name` selector.

        This test plans against an empty local folder. Square must therefore
        never suggest to create or patch resources, only delete them.

        """
        # Only show INFO and above or otherwise this test will produce a
        # humongous amount of logs from all the K8s calls.
        square.square.setup_logging(2)

        # Populate the `Config` structure. All main functions expect this.
        config = Config(
            kubecontext=None,
            kubeconfig=Path("/tmp/kubeconfig-kind.yaml"),

            # Store manifests in this folder.
            folder=tmp_path / 'manifests',
        )

        # ----------------------------------------------------------------------
        # Must find 8 Configmaps in our two test namespaces.
        # ----------------------------------------------------------------------
        config.selectors = Selectors(
            kinds={"Configmap"},
            namespaces=["square-tests-1", "square-tests-2"]
        )
        plan, err = square.plan(config)
        assert not err
        assert len(plan.create) == len(plan.patch) == 0 and len(plan.delete) == 8

        # ----------------------------------------------------------------------
        # Must find 3 Configmaps in "square-tests-1".
        # ----------------------------------------------------------------------
        config.selectors = Selectors(
            kinds={"Configmap"},
            namespaces=["square-tests-1"]
        )
        plan, err = square.plan(config)
        assert not err
        assert len(plan.create) == len(plan.patch) == 0 and len(plan.delete) == 3

        # ----------------------------------------------------------------------
        # Must find exactly one Configmap if we specify it explicitly.
        # ----------------------------------------------------------------------
        config.selectors = Selectors(
            kinds={"Configmap/demoapp-1"},
            namespaces=["square-tests-1"]
        )

        plan, err = square.plan(config)
        assert not err
        assert len(plan.create) == len(plan.patch) == 0 and len(plan.delete) == 1
        meta = [_.meta for _ in plan.delete][0]
        assert meta.kind == "ConfigMap" and meta.name == "demoapp-1"

        # ----------------------------------------------------------------------
        # Must find the explicit ConfigMap if it exists in multiple namespaces.
        # ----------------------------------------------------------------------
        config.selectors = Selectors(
            kinds={"Configmap/demoapp-1"},
            namespaces=[]
        )

        plan, err = square.plan(config)
        assert not err
        assert len(plan.create) == len(plan.patch) == 0 and len(plan.delete) == 2
        metas = [_.meta for _ in plan.delete]
        assert len(metas) == 2
        for meta in metas:
            assert meta.kind == "ConfigMap" and meta.name == "demoapp-1"

    def test_cli_plan_kind_name_resource(self, tmp_path):
        """Use CLI to fetch a specific resource name.

        This test will actually fetch two different ConfigMap names. The first
        one will only exist in a single namespace whereas the second one will
        exist in two namespaces.

        """
        # Convenience.
        man_path = tmp_path / "_other.yaml"

        # Common command line arguments for GET command used in this test.
        common_args = (
            "--folder", str(tmp_path),
            "--kubeconfig", "/tmp/kubeconfig-kind.yaml",
            "--groupby",        # Dump all manifests into a single file.
            "-n"                # Search all namespaces.
        )

        # Fetch all ConfigMaps named `demo-configmap-1`, of which there are two
        # in the demo cluster.
        args = ("square.py", "get", "configmap/demo-configmap-1", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()

        man = list(yaml.safe_load_all(man_path.read_text()))
        assert len(man) == 2
        assert man[0]["metadata"]["name"] == "demo-configmap-1"
        assert man[1]["metadata"]["name"] == "demo-configmap-1"

        # Clear out the manifest folder for the next test.
        man_path.unlink()

        # Fetch all ConfigMaps named `demo-configmap-2`, of which there is only
        # one in the demo cluster.
        args = ("square.py", "get", "configmap/demo-configmap-2", *common_args)
        with mock.patch("sys.argv", args):
            square.main.main()

        man = list(yaml.safe_load_all((tmp_path / "_other.yaml").read_text()))
        assert len(man) == 1
        assert man[0]["metadata"]["name"] == "demo-configmap-2"


@pytest.mark.skipif(not kind_available(), reason="No Integration Test Cluster")
class TestLabels:
    def test_api_plan_labels_simple(self, tmp_path):
        """Plan ConfigMaps based on labels.

        This test plans against an empty local folder. Square must therefore
        never suggest to create or patch resources, only delete them.

        """
        # Only show INFO and above or otherwise this test will produce a
        # humongous amount of logs from all the K8s calls.
        square.square.setup_logging(2)

        # Populate the `Config` structure. All main functions expect this.
        config = Config(
            kubecontext=None,
            kubeconfig=Path("/tmp/kubeconfig-kind.yaml"),

            # Store manifests in this folder.
            folder=tmp_path / 'manifests',
        )

        # ----------------------------------------------------------------------
        # Must find 8 Configmaps without label selectors.
        # ----------------------------------------------------------------------
        config.selectors = Selectors(
            kinds={"Configmap"},
            namespaces=["square-tests-1", "square-tests-2"],
            labels=[],
        )
        plan, err = square.plan(config)
        assert not err and plan.create == plan.patch == []
        assert len(plan.delete) == 8

        # ----------------------------------------------------------------------
        # Must find only 5 Configmaps with label `app=demoapp-1`.
        # ----------------------------------------------------------------------
        config.selectors = Selectors(
            kinds={"Configmap"},
            namespaces=["square-tests-1", "square-tests-2"],
            labels=["app=demoapp-1"],
        )
        plan, err = square.plan(config)
        assert not err and plan.create == plan.patch == []
        assert len(plan.delete) == 5

    def test_api_plan_labels_local_and_remote(self, tmp_path):
        """Plan ConfigMaps based on labels.

        This test creates local dummy manifests to verify that Square selects
        only the manifests with matching labels.

        """
        # Only show INFO and above or otherwise this test will produce a
        # humongous amount of logs from all the K8s calls.
        square.square.setup_logging(2)

        # Populate the `Config` structure. All main functions expect this.
        config = Config(
            kubecontext=None,
            kubeconfig=Path("/tmp/kubeconfig-kind.yaml"),

            # Store manifests in this folder.
            folder=tmp_path / 'manifests',
        )

        # Must find and try to delete our only Configmaps with label `app=demoapp-2`.
        config.selectors = Selectors(
            kinds={"Configmap"},
            namespaces=["square-tests-1", "square-tests-2"],
            labels=["app=demoapp-2"],
        )
        plan, err = square.plan(config)
        assert not err and plan.create == plan.patch == []
        assert len(plan.delete) == 1

        # Sanity check: we do not have a ConfigMap with label "app=new"
        config.selectors = Selectors(
            kinds={"Configmap"},
            namespaces=["square-tests-1", "square-tests-2"],
            labels=["app=new"],
        )
        plan, err = square.plan(config)
        assert not err and plan.create == plan.patch == plan.delete == []

        # ----------------------------------------------------------------------
        # Create local manifest for a new ConfigMap with labels.
        # ----------------------------------------------------------------------
        manifest_new = make_manifest(
            kind="ConfigMap", namespace="square-tests-1",
            name="new", labels={"app": "new"}
        )
        manifest_foo = make_manifest(
            kind="ConfigMap", namespace="square-tests-1",
            name="foo", labels={"app": "foo"}
        )
        config.folder.mkdir()
        fname = config.folder / "manifest.yaml"
        fname.write_text(yaml.dump_all([manifest_new, manifest_foo]))

        # Square must now try to create the one ConfigMap where the labels match.
        plan, err = square.plan(config)
        assert not err and plan.patch == plan.delete == []
        assert len(plan.create) == 1

        # Sanity check: Square must try to create both manifest if we remove
        # the label selectors.
        config.selectors.labels.clear()
        plan, err = square.plan(config)
        assert not err and plan.patch == []
        assert len(plan.create) == 2

    def test_api_plan_same_resource_different_labels(self, tmp_path):
        """Plan a ConfigMaps that has different local and remote labels."""
        # Only show INFO and above or otherwise this test will produce a
        # humongous amount of logs from all the K8s calls.
        square.square.setup_logging(2)

        # Test will only manipulate this one ConfigMap.
        cm_ns = "square-tests-2"
        cm_name = "demo-configmap-2"

        # Populate the `Config` structure. All main functions expect this.
        config = Config(
            kubecontext=None,
            kubeconfig=Path("/tmp/kubeconfig-kind.yaml"),

            # Store manifests in this folder.
            folder=tmp_path / 'manifests',
        )

        # Define path to local ConfigMap manifest. The test will create it later.
        config.folder.mkdir()
        fname = config.folder / "manifest.yaml"

        # ----------------------------------------------------------------------
        # No local ConfigMap manifest: Square must plan to DELETE the ConfigMap.
        # ----------------------------------------------------------------------
        config.selectors = Selectors(
            kinds={"Configmap"},
            namespaces=[cm_ns],
            labels=["app=demoapp-2"],
        )
        plan, err = square.plan(config)
        assert not err
        assert plan.create == plan.patch == []
        assert len(plan.delete) == 1
        assert plan.delete[0].meta.name == cm_name

        # ----------------------------------------------------------------------
        # ConfigMap exists both locally and on server, but with different
        # labels. Square must plan to patch it regardless of whether the labels
        # match only the local or server manifest (or both).
        # ----------------------------------------------------------------------
        for labels in ({}, {"app": "local"}, {"app": "demoapp-2"}):
            manifest = make_manifest(
                kind="ConfigMap", namespace=cm_ns,
                name=cm_name, labels=cast(Dict[str, str], labels),
            )
            fname.write_text(yaml.dump_all([manifest]))
            plan, err = square.plan(config)
            assert not err
            assert plan.create == plan.delete == []
            assert len(plan.patch) == 1
            assert plan.patch[0].meta.name == cm_name
            assert plan.patch[0].meta.namespace == cm_ns


@pytest.mark.skipif(not kind_available(), reason="No Integration Test Cluster")
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
        resources. Then we will use Square to backup it up, delete it and
        finally restore it again.

        """
        # Only show INFO and above or otherwise this test will produce a
        # humongous amount of logs from all the K8s calls.
        square.square.setup_logging(2)

        # Define the resource priority and kinds we have in our workflow
        # manifests. Only target the `test-workflow` labels to avoid problems
        # with non-namespaced resources.
        priorities = [
            "Namespace", "Secret", "ConfigMap", "ClusterRole",
            "ClusterRoleBinding", "Role", "RoleBinding",
        ]
        namespace = "test-workflow"

        config = Config(
            folder=tmp_path / "backup",
            groupby=GroupBy(label="app", order=[]),
            kubecontext=None,
            kubeconfig=Path("/tmp/kubeconfig-kind.yaml"),
            priorities=priorities,
            selectors=Selectors(
                kinds=set(priorities),
                namespaces=[namespace],
                labels=["app=test-workflow"]),
        )

        # ---------------------------------------------------------------------
        # Deploy a new namespace with only a few resources. There are no
        # deployments among them to speed up the deletion of the namespace.
        # ---------------------------------------------------------------------
        sh.kubectl("apply", "--kubeconfig", config.kubeconfig,  # type: ignore
                   "-f", "tests/support/k8s-test-resources.yaml")

        # ---------------------------------------------------------------------
        # Create a plan for "square-tests". The plan must delete all resources
        # because we have not downloaded any manifests yet.
        # ---------------------------------------------------------------------
        plan_1, err = square.square.make_plan(config)
        assert not err
        assert plan_1.create == plan_1.patch == [] and len(plan_1.delete) > 0

        # ---------------------------------------------------------------------
        # Backup all resources. A plan against that backup must be empty.
        # ---------------------------------------------------------------------
        assert not (config.folder / "_other.yaml").exists()
        err = square.square.get_resources(config)
        assert not err and (config.folder / "_other.yaml").exists()

        plan_2, err = square.square.make_plan(config)
        assert not err
        assert plan_2.create == plan_2.patch == plan_2.delete == []

        # ---------------------------------------------------------------------
        # Apply the first plan to delete all resources including the namespace.
        # ---------------------------------------------------------------------
        assert not square.square.apply_plan(config, plan_1)

        # ---------------------------------------------------------------------
        # Wait until K8s has deleted the namespace.
        # ---------------------------------------------------------------------
        for _ in range(120):
            time.sleep(1)
            try:
                sh.kubectl("get", "ns", namespace, "--kubeconfig",  # type: ignore
                           config.kubeconfig)
            except sh.ErrorReturnCode_1:  # type: ignore
                break
        else:
            assert False, f"Could not delete the namespace <{namespace}> in time"

        # ---------------------------------------------------------------------
        # Use backup manifests to restore the namespace.
        # ---------------------------------------------------------------------
        plan_3, err = square.square.make_plan(config)
        assert not err
        assert plan_3.patch == plan_3.delete == [] and len(plan_3.create) > 0

        # Apply the new plan.
        assert not square.square.apply_plan(config, plan_3)

        plan_4, err = square.square.make_plan(config)
        assert not err
        assert plan_4.create == plan_4.patch == plan_4.delete == []

    def test_nondefault_resources(self, tmp_path):
        """Manage an `autoscaling/v1` and `autoscaling/v2beta` at the same time.

        This test is designed to verify that Square will interrogate the
        correct K8s endpoint versions to compute the plan for a resource.

        """
        # Only show INFO and above or otherwise this test will produce a
        # humongous amount of logs from all the K8s calls.
        square.square.setup_logging(2)

        config = Config(
            folder=tmp_path,
            groupby=GroupBy(label="app", order=[]),
            kubecontext=None,
            kubeconfig=Path("/tmp/kubeconfig-kind.yaml"),
            selectors=Selectors(
                kinds={"HorizontalPodAutoscaler"},
                namespaces=["test-hpa"],
                labels=[]),
        )

        # Copy the manifest with the namespace and the two HPAs to the temporary path.
        manifests = list(yaml.safe_load_all(open("tests/support/k8s-test-hpa.yaml")))
        man_path = tmp_path / "manifest.yaml"
        man_path.write_text(yaml.dump_all(manifests))
        assert len(manifests) == 3

        # ---------------------------------------------------------------------
        # Deploy the resources: one namespace with two HPAs in it. On will be
        # deployed via `autoscaling/v1` the other via `autoscaling/v2beta2`.
        # ---------------------------------------------------------------------
        sh.kubectl("apply", "--kubeconfig", config.kubeconfig,  # type: ignore
                   "-f", str(man_path))

        # ---------------------------------------------------------------------
        # The plan must be empty because Square must have interrogated the
        # correct API endpoints for each HPA.
        # ---------------------------------------------------------------------
        plan_1, err = square.square.make_plan(config)
        assert not err
        assert plan_1.create == plan_1.patch == plan_1.delete == []
        del plan_1

        # ---------------------------------------------------------------------
        # Modify the v2beta2 HPA manifest and verify that Square now wants to
        # patch that resource.
        # ---------------------------------------------------------------------
        # Make a change to the manifest and save it.
        tmp_manifests = copy.deepcopy(manifests)
        assert tmp_manifests[2]["apiVersion"] == "autoscaling/v2beta2"
        tmp_manifests[2]["spec"]["metrics"][0]["external"]["metric"]["name"] = "foo"
        man_path.write_text(yaml.dump_all(tmp_manifests))

        # The plan must report one patch.
        plan_2, err = square.square.make_plan(config)
        assert not err
        assert plan_2.create == plan_2.delete == [] and len(plan_2.patch) == 1
        assert plan_2.patch[0].meta.name == "hpav2beta2"
        assert plan_2.patch[0].meta.apiVersion == "autoscaling/v2beta2"
        del plan_2

        # ---------------------------------------------------------------------
        # Delete both HPAs with Square.
        # ---------------------------------------------------------------------
        # Keep only the namespace manifest and save the file.
        tmp_manifests = copy.deepcopy(manifests[:1])
        man_path.write_text(yaml.dump_all(tmp_manifests))

        # Square must now want to delete both HPAs.
        plan_3, err = square.square.make_plan(config)
        assert not err
        assert plan_3.create == plan_3.patch == [] and len(plan_3.delete) == 2
        assert {_.meta.name for _ in plan_3.delete} == {"hpav1", "hpav2beta2"}
        assert not square.square.apply_plan(config, plan_3)
        del plan_3

        # ---------------------------------------------------------------------
        # Re-create both HPAs with Square.
        # ---------------------------------------------------------------------
        # Restore the original manifest file.
        man_path.write_text(yaml.dump_all(manifests))

        # Create a plan. That plan must want to restore both HPAs.
        plan_4, err = square.square.make_plan(config)
        assert not err
        assert plan_4.delete == plan_4.patch == [] and len(plan_4.create) == 2
        assert {_.meta.name for _ in plan_4.create} == {"hpav1", "hpav2beta2"}
        assert {_.meta.apiVersion for _ in plan_4.create} == {
            "autoscaling/v1", "autoscaling/v2beta2"
        }
        assert not square.square.apply_plan(config, plan_4)
        del plan_4

        # Apply the plan.
        plan_5, err = square.square.make_plan(config)
        assert not err
        assert plan_5.create == plan_5.patch == plan_5.delete == []
        del plan_5

        # ---------------------------------------------------------------------
        # Verify that a change in the `apiVersion` would mean a patch.
        # ---------------------------------------------------------------------
        # Manually change the API version of one of the HPAs.
        tmp_manifests = copy.deepcopy(manifests)
        assert tmp_manifests[1]["apiVersion"] == "autoscaling/v1"
        tmp_manifests[1]["apiVersion"] = "autoscaling/v2beta2"
        man_path.write_text(yaml.dump_all(tmp_manifests))

        # Square must now produce a single non-empty patch.
        plan_6, err = square.square.make_plan(config)
        assert not err
        assert plan_6.delete == plan_6.create == [] and len(plan_6.patch) == 1
        assert plan_6.patch[0].meta.name == "hpav1"
        assert plan_6.patch[0].meta.apiVersion == "autoscaling/v2beta2"
