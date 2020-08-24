import copy
import random
import sys
import unittest.mock as mock

import square.k8s as k8s
import square.manio as manio
import square.schemas
import square.square as sq
import yaml
from square.dtypes import (
    DEFAULT_PRIORITIES, Config, DeltaCreate, DeltaDelete, DeltaPatch,
    DeploymentPlan, Filepath, GroupBy, JsonPatch, K8sConfig, MetaManifest,
    Selectors,
)
from square.k8s import resource

from .test_helpers import make_manifest


class TestLogging:
    def test_setup_logging(self):
        """Basic tests - mostly ensure that function runs."""

        # Test function must accept all log levels.
        for level in range(10):
            sq.setup_logging(level)


class TestBasic:
    def test_config_default(self, tmp_path):
        """Default values for Config."""
        assert Config(folder=tmp_path, kubeconfig=tmp_path, kubecontext="ctx") == Config(
            folder=tmp_path,
            kubecontext="ctx",
            kubeconfig=tmp_path,
            selectors=Selectors(kinds=set(DEFAULT_PRIORITIES),
                                namespaces=[],
                                labels=set()),
            priorities=list(DEFAULT_PRIORITIES),
            groupby=GroupBy(label="", order=[]),
            filters={},
        )

    def test_find_namespace_orphans(self):
        """Return all resource manifests that belong to non-existing
        namespaces.

        This function will be useful to sanity check the local deployments
        manifest to avoid cases where users define resources in a namespace but
        forget to define that namespace (or mis-spell it).

        """
        fun = sq.find_namespace_orphans

        # Two deployments in the same non-existing Namespace. Both are orphaned
        # because the namespace `ns1` does not exist.
        man = {
            MetaManifest('v1', 'Deployment', 'ns1', 'foo'),
            MetaManifest('v1', 'Deployment', 'ns1', 'bar'),
        }
        assert fun(man) == (man, True)

        # Two namespaces - neither is orphaned by definition.
        man = {
            MetaManifest('v1', 'Namespace', None, 'ns1'),
            MetaManifest('v1', 'Namespace', None, 'ns2'),
        }
        assert fun(man) == (set(), True)

        # Two deployments, only one of which is inside a defined Namespace.
        man = {
            MetaManifest('v1', 'Deployment', 'ns1', 'foo'),
            MetaManifest('v1', 'Deployment', 'ns2', 'bar'),
            MetaManifest('v1', 'Namespace', None, 'ns1'),
        }
        assert fun(man) == ({MetaManifest('v1', 'Deployment', 'ns2', 'bar')}, True)

    def test_show_plan(self):
        """Just verify it runs.

        There is nothing really to tests here because the function only prints
        strings to the terminal. Therefore, we will merely ensure that all code
        paths run without error.

        """
        meta = manio.make_meta(make_manifest("Deployment", "ns", "name"))
        patch = JsonPatch(
            url="url",
            ops=[
                {'op': 'remove', 'path': '/metadata/labels/old'},
                {'op': 'add', 'path': '/metadata/labels/new', 'value': 'new'}
            ],
        )
        plan = DeploymentPlan(
            create=[DeltaCreate(meta, "url", "manifest")],
            patch=[
                DeltaPatch(meta, "", patch),
                DeltaPatch(meta, "  normal\n+  add\n-  remove", patch)
            ],
            delete=[DeltaDelete(meta, "url", "manifest")],
        )
        assert sq.show_plan(plan) is False

    def test_translate_resource_kinds(self, k8sconfig):
        """Translate various spellings in `selectors.kinds`"""
        cfg = Config(
            folder=Filepath('/tmp'),
            kubeconfig="",
            kubecontext=None,
            selectors=Selectors(
                kinds={"svc", 'DEPLOYMENT', "Secret"},
                namespaces=['default'],
                labels=["app=morty", "foo=bar"],
            ),
            groupby=GroupBy("", []),
            priorities=["ns", "DEPLOYMENT"],
        )

        # Convert the resource names to their correct K8s kind.
        ret = sq.translate_resource_kinds(cfg, k8sconfig)
        assert ret.selectors.kinds == {"Service", "Deployment", "Secret"}
        assert ret.priorities == ["Namespace", "Deployment"]

        # Add two invalid resource names. This must succeed but return the
        # resource names without having changed them.
        cfg.selectors.kinds.clear()
        cfg.selectors.kinds.update({"invalid", "k8s-resource-kind"})
        cfg.priorities.clear()
        cfg.priorities.extend(["invalid", "k8s-resource-kind"])
        ret = sq.translate_resource_kinds(cfg, k8sconfig)
        assert ret.selectors.kinds == {"invalid", "k8s-resource-kind"}
        assert ret.priorities == ["invalid", "k8s-resource-kind"]

    def test_sanity_check_labels(self, config):
        """Main entry point functions must abort if any labels are invalid."""
        # Dummy plan.
        plan = DeploymentPlan(tuple(), tuple(), tuple())

        # Test a variety of invalid labels.
        invalid_labels = [
            ["foo=bar=invalid"],
            ["is=valid", "not==valid"],
            [""],
            ["foo=bar", ""],
        ]
        for labels in invalid_labels:
            config.selectors.labels = labels
            assert sq.get_resources(config) is True
            assert sq.apply_plan(config, plan) is True
            assert sq.make_plan(config) == (plan, True)


class TestLoadConfig:
    def test_load_config(self):
        """Load and parse configuration file."""
        # Load the sample that ships with Square.
        fname = Filepath("tests/support/config.yaml")
        cfg, err = sq.load_config(fname)
        assert not err and isinstance(cfg, Config)

        assert cfg.folder == fname.parent.absolute() / "some/path"
        assert cfg.kubeconfig == Filepath("/path/to/kubeconfig")
        assert cfg.kubecontext is None
        assert cfg.priorities == list(DEFAULT_PRIORITIES)
        assert cfg.selectors.kinds == set(DEFAULT_PRIORITIES)
        assert cfg.selectors.namespaces == ["default", "kube-system"]
        assert cfg.selectors.labels == ["app=square"]
        assert set(cfg.filters.keys()) == {
            "ConfigMap", "Deployment", "HorizontalPodAutoscaler", "Service"
        }

        # The _common_ filters must have been merged into all filters. Here we
        # verify it for a Deployment because its filter definition in the
        # config file was empty, which makes this easy to verify.
        assert cfg.filters["Deployment"] == square.schemas.default()

        cfg2, err = sq.load_config(str(fname))
        assert not err and cfg == cfg2

    def test_load_config_folder_paths(self, tmp_path):
        """The folder paths must always be relative to the config file."""
        fname = tmp_path / ".square.yaml"
        fname_ref = Filepath("tests/support/config.yaml")

        # The parsed folder must point to "tmp_path".
        ref = yaml.safe_load(fname_ref.read_text())
        fname.write_text(yaml.dump(ref))
        cfg, err = sq.load_config(fname)
        assert not err and cfg.folder == tmp_path / "some/path"

        # The parsed folder must point to "tmp_path/folder".
        ref = yaml.safe_load(fname_ref.read_text())
        ref["folder"] = "my-folder"
        fname.write_text(yaml.dump(ref))
        cfg, err = sq.load_config(fname)
        assert not err and cfg.folder == tmp_path / "my-folder"

        # An absolute path must ignore the position of ".square.yaml".
        # No idea how to handle this on Windows.
        if not sys.platform.startswith("win"):
            ref = yaml.safe_load(fname_ref.read_text())
            ref["folder"] = "/absolute/path"
            fname.write_text(yaml.dump(ref))
            cfg, err = sq.load_config(fname)
            assert not err and cfg.folder == Filepath("/absolute/path")

    def test_common_filters(self, tmp_path):
        """Deal with empty or non-existing `_common_` filter."""
        fname_ref = Filepath("tests/support/config.yaml")

        # ----------------------------------------------------------------------
        # Empty _common_ filters.
        # ----------------------------------------------------------------------
        # Clear the "_common_" filter and save the configuration again.
        ref = yaml.safe_load(fname_ref.read_text())
        ref["filters"]["_common_"].clear()
        fout = tmp_path / "corrupt.yaml"
        fout.write_text(yaml.dump(ref))

        # Load the new configuration. This must succeed and the filters must
        # match the ones defined in the file because there the "_common_"
        # filter was empty. NOTE: `cfg.filters` must *not* contain the _common_
        # filter.
        cfg, err = sq.load_config(fout)
        del ref["filters"]["_common_"]
        assert not err and ref["filters"] == cfg.filters

        # ----------------------------------------------------------------------
        # Missing _common_ filters.
        # ----------------------------------------------------------------------
        # Remove the "_common_" filter and save the configuration again.
        ref = yaml.safe_load(fname_ref.read_text())
        del ref["filters"]["_common_"]
        fout = tmp_path / "corrupt.yaml"
        fout.write_text(yaml.dump(ref))

        # Load the new configuration. This must succeed and the filters must
        # match the ones defined in the file because there was no "_common_"
        # filter to merge.
        cfg, err = sq.load_config(fout)
        assert not err and ref["filters"] == cfg.filters

    def test_load_config_err(self, tmp_path):
        """Gracefully handle missing file, corrupt content etc."""
        # Must gracefully handle a corrupt configuration file.
        fname = tmp_path / "does-not-exist.yaml"
        _, err = sq.load_config(fname)
        assert err

        # YAML error.
        fname = tmp_path / "corrupt-yaml.yaml"
        fname.write_text("[foo")
        _, err = sq.load_config(fname)
        assert err

        # Does not match the definition of `dtypes.Config`.
        fname = tmp_path / "invalid-pydantic-schema.yaml"
        fname.write_text("foo: bar")
        _, err = sq.load_config(fname)
        assert err

        # YAML file is valid but not a map. This special case is important
        # because the test function will expand the content as **kwargs.
        fname = tmp_path / "invalid-pydantic-schema.yaml"
        fname.write_text("")
        _, err = sq.load_config(fname)
        assert err

        # Load the sample configuration and corrupt the label selector. Instead
        # of a list of 2-tuples we make it a list of 3-tuples.
        cfg = yaml.safe_load(Filepath("tests/support/config.yaml").read_text())
        cfg["selectors"]["labels"] = [["foo", "bar", "foobar"]]
        fout = tmp_path / "corrupt.yaml"
        fout.write_text(yaml.dump(cfg))
        _, err = sq.load_config(fout)
        assert err


class TestPartition:
    def test_partition_manifests_patch(self):
        """Local and server manifests match.

        If all resource exist both locally and remotely then nothing needs to
        be created or deleted. However, the resources may need patching but
        that is not something `partition_manifests` concerns itself with.

        """
        # Local and cluster manifests are identical - the Plan must not
        # create/add anything but mark all resources for (possible)
        # patching.
        local_man = cluster_man = {
            MetaManifest('v1', 'Namespace', None, 'ns3'): "0",
            MetaManifest('v1', 'Namespace', None, 'ns1'): "1",
            MetaManifest('v1', 'Deployment', 'ns2', 'bar'): "2",
            MetaManifest('v1', 'Namespace', None, 'ns2'): "3",
            MetaManifest('v1', 'Deployment', 'ns1', 'foo'): "4",
        }
        plan = DeploymentPlan(create=[], patch=list(local_man.keys()), delete=[])
        assert sq.partition_manifests(local_man, cluster_man) == (plan, False)

    def test_partition_manifests_add_delete(self):
        """Local and server manifests are orthogonal sets.

        This must produce a plan where all local resources will be created, all
        cluster resources deleted and none patched.

        """
        fun = sq.partition_manifests

        # Local and cluster manifests are orthogonal.
        local_man = {
            MetaManifest('v1', 'Deployment', 'ns2', 'bar'): "0",
            MetaManifest('v1', 'Namespace', None, 'ns2'): "1",
        }
        cluster_man = {
            MetaManifest('v1', 'Deployment', 'ns1', 'foo'): "2",
            MetaManifest('v1', 'Namespace', None, 'ns1'): "3",
            MetaManifest('v1', 'Namespace', None, 'ns3'): "4",
        }
        plan = DeploymentPlan(
            create=[
                MetaManifest('v1', 'Deployment', 'ns2', 'bar'),
                MetaManifest('v1', 'Namespace', None, 'ns2'),
            ],
            patch=[],
            delete=[
                MetaManifest('v1', 'Deployment', 'ns1', 'foo'),
                MetaManifest('v1', 'Namespace', None, 'ns1'),
                MetaManifest('v1', 'Namespace', None, 'ns3'),
            ]
        )
        assert fun(local_man, cluster_man) == (plan, False)

    def test_partition_manifests_patch_delete(self):
        """Create plan with resources to delete and patch.

        The local manifests are a strict subset of the cluster. The deployment
        plan must therefore not create any resources, delete everything absent
        from the local manifests and mark the rest for patching.

        """
        fun = sq.partition_manifests

        # The local manifests are a subset of the server's. Therefore, the plan
        # must contain patches for those resources that exist locally and on
        # the server. All the other manifest on the server are obsolete.
        local_man = {
            MetaManifest('v1', 'Deployment', 'ns2', 'bar1'): "0",
            MetaManifest('v1', 'Namespace', None, 'ns2'): "1",
        }
        cluster_man = {
            MetaManifest('v1', 'Deployment', 'ns1', 'foo'): "2",
            MetaManifest('v1', 'Deployment', 'ns2', 'bar1'): "3",
            MetaManifest('v1', 'Deployment', 'ns2', 'bar2'): "4",
            MetaManifest('v1', 'Namespace', None, 'ns1'): "5",
            MetaManifest('v1', 'Namespace', None, 'ns2'): "6",
            MetaManifest('v1', 'Namespace', None, 'ns3'): "7",
        }
        plan = DeploymentPlan(
            create=[],
            patch=[
                MetaManifest('v1', 'Deployment', 'ns2', 'bar1'),
                MetaManifest('v1', 'Namespace', None, 'ns2'),
            ],
            delete=[
                MetaManifest('v1', 'Deployment', 'ns1', 'foo'),
                MetaManifest('v1', 'Deployment', 'ns2', 'bar2'),
                MetaManifest('v1', 'Namespace', None, 'ns1'),
                MetaManifest('v1', 'Namespace', None, 'ns3'),
            ]
        )
        assert fun(local_man, cluster_man) == (plan, False)


class TestPatchK8s:
    def test_make_patch_empty(self, config, k8sconfig):
        """Basic test: compute patch between two identical resources."""
        # Setup.
        kind, ns, name = 'Deployment', 'ns', 'foo'

        # PATCH URLs require the resource name at the end of the request path.
        url = resource(k8sconfig, MetaManifest("apps/v1", kind, ns, name))[0].url

        # The patch must be empty for identical manifests.
        loc = srv = make_manifest(kind, ns, name)
        data, err = sq.make_patch(config, k8sconfig, loc, srv)
        assert (data, err) == (JsonPatch(url, []), False)
        assert isinstance(data, JsonPatch)

    def test_make_patch_incompatible(self, config, k8sconfig):
        """Must not try to compute diffs for incompatible manifests.

        For instance, refuse to compute a patch when one manifest has kind
        "Namespace" and the other "Deployment". The same is true for
        "apiVersion", "metadata.name" and "metadata.namespace".

        """
        # Demo Deployment manifest.
        srv = make_manifest('Deployment', 'Namespace', 'name')
        err_resp = (JsonPatch("", []), True)

        # `apiVersion` must match.
        loc = copy.deepcopy(srv)
        loc['apiVersion'] = 'mismatch'
        assert sq.make_patch(config, k8sconfig, loc, srv) == err_resp

        # `kind` must match.
        loc = copy.deepcopy(srv)
        loc['kind'] = 'Mismatch'
        assert sq.make_patch(config, k8sconfig, loc, srv) == err_resp

        # `name` must match.
        loc = copy.deepcopy(srv)
        loc['metadata']['name'] = 'mismatch'
        assert sq.make_patch(config, k8sconfig, loc, srv) == err_resp

        # `namespace` must match.
        loc = copy.deepcopy(srv)
        loc['metadata']['namespace'] = 'mismatch'
        assert sq.make_patch(config, k8sconfig, loc, srv) == err_resp

    def test_make_patch_special(self, config, k8sconfig):
        """Namespace, ClusterRole(Bindings) etc are special.

        What makes them special is that they exist outside namespaces.
        Therefore, they will/must not contain a `metadata.Namespace` attribute
        and require special treatment in `make_patch`.

        """
        for kind in ["Namespace", "ClusterRole"]:
            meta = manio.make_meta(make_manifest(kind, None, "name"))

            # Determine the resource path so we can verify it later.
            url = resource(k8sconfig, meta)[0].url

            # The patch between two identical manifests must be empty but valid.
            loc = srv = make_manifest(kind, None, "name")
            assert sq.make_patch(config, k8sconfig, loc, srv) == ((url, []), False)

            # Create two almost identical manifests, except the second one has
            # different `metadata.labels`. This must succeed.
            loc = make_manifest(kind, None, "name")
            srv = copy.deepcopy(loc)
            loc['metadata']['labels'] = {"key": "value"}

            data, err = sq.make_patch(config, k8sconfig, loc, srv)
            assert err is False and len(data) > 0

    @mock.patch.object(k8s, "resource")
    def test_make_patch_error_resource(self, m_url, config, k8sconfig):
        """Coverage gap: simulate `resource` error."""
        # Simulate `resource` error.
        m_url.return_value = (None, True)

        # Test function must return with error.
        loc = srv = make_manifest("Deployment", "ns", "foo")
        assert sq.make_patch(config, k8sconfig, loc, srv) == (JsonPatch("", []), True)


class TestMatchApiVersions:
    @mock.patch.object(square.manio, "download_single")
    def test_match_api_version_basic(self, m_fetch, k8sconfig):
        """Define tow resources and verify the test function downloads the one
        where the `apiVersion` does not match.

        """
        # Create local and server manifests. Both specify the same two resources
        # but the Deployment uses different `apiVersions`.
        meta_deploy_loc = MetaManifest("extensions/v1beta1", "Deployment", "ns", "name")
        meta_deploy_srv = MetaManifest("apps/v1", "Deployment", "ns", "name")
        local = {
            MetaManifest("v1", "Namespace", None, "ns1"): {"ns-loc"},
            meta_deploy_loc: {"dply-loc"},
        }
        server_in = {
            MetaManifest("v1", "Namespace", None, "ns1"): {"ns-srv"},
            meta_deploy_srv: {"orig-srv"},
        }

        # Mock the resource download to supply it from the correct API endpoint.
        resource, err = square.k8s.resource(k8sconfig, meta_deploy_loc)
        assert not err
        m_fetch.return_value = (meta_deploy_loc, {"new-srv"}, False)
        del err

        # Test function must have re-downloaded the Deployment from the
        # `extensions/v1beta1` endpoint.
        srv, err = square.square.match_api_version(k8sconfig, local, server_in)
        assert not err and srv == {
            MetaManifest("v1", "Namespace", None, "ns1"): {"ns-srv"},
            MetaManifest("extensions/v1beta1", "Deployment", "ns", "name"): {"new-srv"},
        }

        # Must have downloaded the deployments.
        m_fetch.assert_called_once_with(k8sconfig, resource)

    @mock.patch.object(square.manio, "download_single")
    def test_match_api_version_namespace(self, m_fetch, k8sconfig):
        """Define a set of resources and verify the function downloads the ones
        where the `apiVersion` fields do not match.

        """
        # Create local and server manifests. Both specify the same two resources
        # but the Deployment uses different `apiVersions`.
        local = {
            MetaManifest("apps/v1beta1", "Deployment", "name", "ns1"): {"deploy-1"},
            MetaManifest("apps/v1beta2", "Deployment", "name", "ns2"): {"deploy-2"},
        }
        server_in = {
            # Same as in `local`
            MetaManifest("apps/v1beta1", "Deployment", "name", "ns1"): {"deploy-1"},

            # Different than in `local`.
            MetaManifest("apps/v1beta1", "Deployment", "name", "ns2"): {"deploy-2"},
        }

        # Mock the resource download to supply it from the correct API endpoint.
        meta = MetaManifest("apps/v1beta2", "Deployment", "name", "ns2")
        assert meta in local
        resource, err = square.k8s.resource(k8sconfig, meta)
        assert not err
        m_fetch.return_value = (meta, {"new-deploy-2"}, False)
        del err

        # Test function must have re-downloaded the Deployment from the
        # `extensions/v1beta1` endpoint.
        srv, err = square.square.match_api_version(k8sconfig, local, server_in)
        assert not err and srv == {
            MetaManifest("apps/v1beta1", "Deployment", "name", "ns1"): {"deploy-1"},
            MetaManifest("apps/v1beta2", "Deployment", "name", "ns2"): {"new-deploy-2"},
        }

        # Must have downloaded the deployments.
        m_fetch.assert_called_once_with(k8sconfig, resource)

    @mock.patch.object(square.manio, "download_single")
    def test_match_api_version_multi(self, m_fetch, k8sconfig):
        """Mix multiple deployments, some of which need re-downloading."""
        # Create local and server manifests. Both specify the same two resources
        # but the Deployment uses different `apiVersions`.
        local_in = {
            # These two exist on server as well (same API version).
            MetaManifest("apps/v1", "Deployment", "ns", "name-1"): {"loc-deploy-1"},
            MetaManifest("apps/v1beta1", "Deployment", "ns", "name-2"): {"loc-deploy-2"},

            # Also exists on server but with different version.
            MetaManifest("apps/v1beta1", "Deployment", "ns", "name-3"): {"loc-deploy-3"},
        }
        server_in = {
            # These two exist locally as well (same API version).
            MetaManifest("apps/v1", "Deployment", "ns", "name-1"): {"srv-deploy-1"},
            MetaManifest("apps/v1beta1", "Deployment", "ns", "name-2"): {"srv-deploy-2"},

            # Also exists locally but with different version.
            MetaManifest("apps/v1beta2", "Deployment", "ns", "name-3"): {"loc-deploy-3"},
        }

        # Mock the resource download to supply it from the correct API endpoint.
        meta = MetaManifest("apps/v1beta1", "Deployment", "ns", "name-3")
        resource, err = square.k8s.resource(k8sconfig, meta)
        assert not err
        m_fetch.return_value = (meta, {"new-deploy-3"}, False)
        del err

        # Test function must have re-downloaded the Deployment "name-3" from the
        # `apps/v1beta1` endpoint because that is what the local manifest uses.
        srv, err = square.square.match_api_version(k8sconfig, local_in, server_in)
        assert not err and srv == {
            MetaManifest("apps/v1", "Deployment", "ns", "name-1"): {"srv-deploy-1"},
            MetaManifest("apps/v1beta1", "Deployment", "ns", "name-2"): {"srv-deploy-2"},

            # This must have been downloaded.
            MetaManifest("apps/v1beta1", "Deployment", "ns", "name-3"): {"new-deploy-3"},
        }

        # Must have downloaded exactly one deployment, namely `name-3`.
        m_fetch.assert_called_once_with(k8sconfig, resource)

    @mock.patch.object(square.manio, "download_single")
    def test_match_api_version_nothing_to_do(self, m_fetch, k8sconfig):
        """Test various cases where the function must not do anything.

        There are two cases where it must not download a resource form K8s again:
          1) Local/Server use identical API endpoints the resource.
          2) Resource exists either on server or locally but not both.

        """
        fun = square.square.match_api_version

        # Must not have downloaded anything.
        srv, err = fun(k8sconfig, {}, {})
        assert not err and srv == {}
        assert not m_fetch.called

        # Local and server manifests are identical - must not synchronise anything.
        local_in = {
            MetaManifest("v1", "Namespace", None, "ns1"): {"ns-loc"},
            MetaManifest("apps/v1", "Deployment", "ns", "name"): {"dply-loc"},
        }
        srv, err = fun(k8sconfig, local_in, local_in)
        assert not err and srv == local_in
        assert not m_fetch.called

        # Local- and server manifests have identical Service resource but two
        # completely different deployments. Must not sync anything because the
        # deployments are actually different resources.
        local_in = {
            MetaManifest("v1", "Service", "svc-name", "ns1"): {"ns-loc"},
            MetaManifest("apps/v1", "Deployment", "ns", "foo"): {"dply-loc"},
        }
        server_in = {
            MetaManifest("v1", "Service", "svc-name", "ns1"): {"ns-srv"},
            MetaManifest("extensions/v1beta1", "Deployment", "ns", "bar"): {"orig-srv"},
        }
        srv, err = fun(k8sconfig, local_in, server_in)
        assert not err and srv == server_in
        assert not m_fetch.called

        # Local- and server manifests have matching Deployments in two
        # different namespaces.
        local_in = {
            MetaManifest("apps/v1beta1", "Deployment", "name", "ns1"): {"deploy-1"},
            MetaManifest("apps/v1beta2", "Deployment", "name", "ns2"): {"deploy-2"},
        }
        server_in = {
            MetaManifest("apps/v1beta1", "Deployment", "name", "ns1"): {"deploy-1"},
            MetaManifest("apps/v1beta2", "Deployment", "name", "ns2"): {"deploy-2"},
        }
        srv, err = fun(k8sconfig, local_in, server_in)
        assert not err and srv == server_in
        assert not m_fetch.called


class TestPlan:
    def test_make_patch_ok(self, config, k8sconfig):
        """Compute patch between two manifests.

        This test function first verifies that the patch between two identical
        manifests is empty. The second used two manifests that have different
        labels. This must produce two patch operations, one to remove the old
        label and one to add the new ones.

        """
        # Two valid manifests.
        kind, namespace, name = "Deployment", "namespace", "name"
        srv = make_manifest(kind, namespace, name)
        loc = make_manifest(kind, namespace, name)
        srv["metadata"]["labels"] = {"old": "old"}
        loc["metadata"]["labels"] = {"new": "new"}

        # The Patch between two identical manifests must be a No-Op.
        res, err = resource(k8sconfig, MetaManifest("apps/v1", kind, namespace, name))
        assert not err
        expected = JsonPatch(url=res.url, ops=[])
        assert sq.make_patch(config, k8sconfig, loc, loc) == (expected, False)

        # The patch between `srv` and `loc` must remove the old label and add
        # the new one.
        expected = JsonPatch(
            url=res.url,
            ops=[
                {'op': 'remove', 'path': '/metadata/labels/old'},
                {'op': 'add', 'path': '/metadata/labels/new', 'value': 'new'}
            ]
        )
        assert sq.make_patch(config, k8sconfig, loc, srv) == (expected, False)

    def test_make_patch_err(self, config, k8sconfig):
        """Verify error cases with invalid or incompatible manifests."""
        err_resp = (JsonPatch("", []), True)

        kind, namespace, name = "Deployment", "namespace", "name"
        valid = make_manifest(kind, namespace, name)

        # Must handle `resource` errors.
        with mock.patch.object(sq.k8s, "resource") as m_url:
            m_url.return_value = (None, True)
            assert sq.make_patch(config, k8sconfig, valid, valid) == err_resp

        # Must handle incompatible manifests, ie manifests that do not belong
        # to the same resource.
        valid_a = make_manifest(kind, namespace, "bar")
        valid_b = make_manifest(kind, namespace, "foo")
        assert sq.make_patch(config, k8sconfig, valid_a, valid_b) == err_resp

    def test_sort_plan(self, config):
        # Dummy MetaManifests that we will use in our test plan.
        meta_ns0 = MetaManifest('v1', 'Namespace', None, 'ns0')
        meta_ns1 = MetaManifest('v1', 'Namespace', None, 'ns1')
        meta_svc0 = MetaManifest('v1', 'Service', "ns0", 'svc0')
        meta_svc1 = MetaManifest('v1', 'Service', "ns1", 'svc1')
        meta_dpl0 = MetaManifest('apps/v1', 'Deployment', 'ns0', 'deploy_0')
        meta_dpl1 = MetaManifest('apps/v1', 'Deployment', 'ns1', 'deploy_1')

        expected = DeploymentPlan(
            create=[
                DeltaCreate(meta_ns0, None, None),
                DeltaCreate(meta_ns1, None, None),
                DeltaCreate(meta_svc0, None, None),
                DeltaCreate(meta_svc1, None, None),
                DeltaCreate(meta_dpl0, None, None),
                DeltaCreate(meta_dpl1, None, None),
            ],
            patch=[
                DeltaCreate(meta_ns0, None, None),
                DeltaCreate(meta_ns1, None, None),
                DeltaCreate(meta_svc0, None, None),
                DeltaCreate(meta_svc1, None, None),
                DeltaCreate(meta_dpl0, None, None),
                DeltaCreate(meta_dpl1, None, None),
            ],
            delete=[
                DeltaCreate(meta_dpl1, None, None),
                DeltaCreate(meta_dpl0, None, None),
                DeltaCreate(meta_svc1, None, None),
                DeltaCreate(meta_svc0, None, None),
                DeltaCreate(meta_ns1, None, None),
                DeltaCreate(meta_ns0, None, None),
            ],
        )

        config.priorities = ["Namespace", "Service", "Deployment"]
        plan = copy.deepcopy(expected)
        for i in range(10):
            random.shuffle(plan.create)
            random.shuffle(plan.patch)
            random.shuffle(plan.delete)
            ret, err = sq.sort_plan(config, plan)
            assert not err
            assert ret.create == expected.create
            assert ret.delete == expected.delete
            assert ret.patch == plan.patch

        # Service must be last because it is not in the priority list.
        expected = DeploymentPlan(
            create=[
                DeltaCreate(meta_ns0, None, None),
                DeltaCreate(meta_ns1, None, None),
                DeltaCreate(meta_dpl0, None, None),
                DeltaCreate(meta_svc1, None, None),
            ],
            patch=[
                DeltaCreate(meta_ns0, None, None),
                DeltaCreate(meta_ns1, None, None),
                DeltaCreate(meta_svc0, None, None),
                DeltaCreate(meta_svc1, None, None),
                DeltaCreate(meta_dpl0, None, None),
                DeltaCreate(meta_dpl1, None, None),
            ],
            delete=[
                DeltaCreate(meta_svc0, None, None),
                DeltaCreate(meta_dpl1, None, None),
                DeltaCreate(meta_ns1, None, None),
                DeltaCreate(meta_ns0, None, None),
            ],
        )
        config.priorities = ["Namespace", "Deployment"]
        plan = copy.deepcopy(expected)
        for i in range(10):
            random.shuffle(plan.create)
            random.shuffle(plan.patch)
            random.shuffle(plan.delete)
            ret, err = sq.sort_plan(config, plan)
            assert not err
            assert ret.create == expected.create
            assert ret.delete == expected.delete
            assert ret.patch == plan.patch

    def test_sort_plan_err(self, config):
        """Do not sort anything unless all `priorities` are unique."""
        # The "Namespace" resource is listed twice - error.
        config.priorities = ["Namespace", "Service", "Namespace"]

        plan = DeploymentPlan(create=[], patch=[], delete=[])
        ret, err = sq.sort_plan(config, plan)
        assert err and ret == plan

    def test_compile_plan_create_delete_ok(self, config, k8sconfig):
        """Test a plan that creates and deletes resource, but not patch any.

        To do this, the local and server resources are all distinct. As a
        result, the returned plan must dictate that all local resources shall
        be created, all server resources deleted, and none patched.

        """
        # Local: defines Namespace "ns1" with 1 deployment.
        meta = [
            MetaManifest('v1', 'Namespace', None, 'ns1'),
            MetaManifest('apps/v1', 'Deployment', 'ns1', 'res_0'),

            # Server: has a Namespace "ns2" with 2 deployments.
            MetaManifest('v1', 'Namespace', None, 'ns2'),
            MetaManifest('apps/v1', 'Deployment', 'ns2', 'res_1'),
            MetaManifest('apps/v1', 'Deployment', 'ns2', 'res_2'),
        ]

        # Determine the K8sResource for all involved resources. Also verify
        # that all resources specify a valid API group.
        res = [resource(k8sconfig, _._replace(name="")) for _ in meta]
        assert not any([_[1] for _ in res])
        res = [_[0] for _ in res]

        # Compile local and server manifests. Their resources have no overlap.
        # This will ensure that we have to create all the local resources,
        # delete all the server resources, and patch nothing.
        loc_man = {_: make_manifest(_.kind, _.namespace, _.name) for _ in meta[:2]}
        srv_man = {_: make_manifest(_.kind, _.namespace, _.name) for _ in meta[2:]}

        # The resources require a manifest to specify the terms of deletion.
        # This is currently hard coded into the function.
        del_opts = {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "gracePeriodSeconds": 0,
            "orphanDependents": False,
        }

        # Resources declared in local files must be created and server resources deleted.
        expected = DeploymentPlan(
            create=[
                DeltaCreate(meta[0], res[0].url, loc_man[meta[0]]),
                DeltaCreate(meta[1], res[1].url, loc_man[meta[1]]),
            ],
            patch=[],
            delete=[
                DeltaDelete(meta[2], res[2].url + "/" + meta[2].name, del_opts),
                DeltaDelete(meta[3], res[3].url + "/" + meta[3].name, del_opts),
                DeltaDelete(meta[4], res[4].url + "/" + meta[4].name, del_opts),
            ],
        )
        ret, err = sq.compile_plan(config, k8sconfig, loc_man, srv_man)
        assert ret.create == expected.create
        assert (ret, err) == (expected, False)

    @mock.patch.object(sq, "match_api_version")
    def test_compile_plan_create_delete_err(self, m_part, config, k8sconfig):
        """Simulate `resource` errors."""
        err_resp = (DeploymentPlan(tuple(), tuple(), tuple()), True)

        # Valid ManifestMeta and dummy manifest dict.
        man = make_manifest("Deployment", "ns", "name")
        meta = manio.make_meta(man)
        man = {meta: man}

        # Pretend we only have to "create" resources and then trigger the
        # `resource` error in its code path.
        m_part.return_value = (
            DeploymentPlan(create=[meta], patch=[], delete=[]),
            False,
        )

        # We must not be able to compile a plan because of the `resource` error.
        with mock.patch.object(sq.k8s, "resource") as m_url:
            m_url.return_value = (None, True)
            assert sq.compile_plan(config, k8sconfig, man, man) == err_resp

        # Pretend we only have to "delete" resources, and then trigger the
        # `resource` error in its code path.
        m_part.return_value = (
            DeploymentPlan(create=[], patch=[], delete=[meta]),
            False,
        )
        with mock.patch.object(sq.k8s, "resource") as m_url:
            m_url.return_value = (None, True)
            assert sq.compile_plan(config, k8sconfig, man, man) == err_resp

    def test_compile_plan_patch_no_diff(self, config, k8sconfig):
        """The plan must be empty if the local and server manifests are too."""
        # Define two namespaces with 1 deployment in each.
        meta = [
            MetaManifest('v1', 'Namespace', None, 'ns1'),
            MetaManifest('apps/v1', 'Deployment', 'ns1', 'res_0'),
            MetaManifest('v1', 'Namespace', None, 'ns2'),
            MetaManifest('apps/v1', 'Deployment', 'ns2', 'res_1'),
        ]

        # Local and server manifests are identical. The plan must therefore
        # only nominate patches but nothing to create or delete.
        src = {_: make_manifest(_.kind, _.namespace, _.name) for _ in meta}

        expected = DeploymentPlan(create=[], patch=[], delete=[])
        assert sq.compile_plan(config, k8sconfig, src, src) == (expected, False)

    def test_compile_plan_invalid_api_version(self, config, k8sconfig):
        """Test a plan that patches no resources.

        The local and server manifests are identical except for the API
        version. The plan must still be empty because Square adapts to the
        local manifests to the default API group.

        """
        # Define a namespaces with an Ingress. The Ingress uses the legacy API group.
        meta = [
            MetaManifest("invalid", "Deployment", "ns", "name"),
        ]

        # Local and server manifests will be identical.
        src = {_: make_manifest(_.kind, _.namespace, _.name) for _ in meta}

        # The plan must fail because the API group is invalid.
        ret = sq.compile_plan(config, k8sconfig, src, src)
        assert ret == (DeploymentPlan(tuple(), tuple(), tuple()), True)

    def test_compile_plan_patch_with_diff(self, config, k8sconfig):
        """Test a plan that patches all resources.

        To do this, the local and server resources are identical. As a
        result, the returned plan must nominate all manifests for patching, and
        none to create and delete.

        """
        # Define a single resource.
        meta = MetaManifest('v1', 'Namespace', None, 'ns1')

        # Local and server manifests have the same resources but their
        # definition differs. This will ensure a non-empty patch in the plan.
        loc_man = {meta: make_manifest("Namespace", None, "ns1")}
        srv_man = {meta: make_manifest("Namespace", None, "ns1")}
        loc_man[meta]["metadata"]["labels"] = {"foo": "foo"}
        srv_man[meta]["metadata"]["labels"] = {"bar": "bar"}

        # Compute the JSON patch and textual diff to populate the expected
        # output structure below.
        patch, err = sq.make_patch(config, k8sconfig, loc_man[meta], srv_man[meta])
        assert not err
        diff_str, err = manio.diff(config, k8sconfig, loc_man[meta], srv_man[meta])
        assert not err

        # Verify the test function returns the correct Patch and diff.
        expected = DeploymentPlan(
            create=[],
            patch=[DeltaPatch(meta, diff_str, patch)],
            delete=[]
        )
        ret = sq.compile_plan(config, k8sconfig, loc_man, srv_man)
        assert ret == (expected, False)

    @mock.patch.object(sq, "partition_manifests")
    @mock.patch.object(manio, "diff")
    @mock.patch.object(sq, "make_patch")
    def test_compile_plan_err(self, m_apply, m_plan, m_part, config, k8sconfig):
        """Use mocks for the internal function calls to simulate errors."""
        err_resp = (DeploymentPlan(tuple(), tuple(), tuple()), True)

        # Define a single resource and valid dummy return value for
        # `sq.partition_manifests`.
        meta = MetaManifest('v1', 'Namespace', None, 'ns1')
        plan = DeploymentPlan(create=[], patch=[meta], delete=[])

        # Local and server manifests have the same resources but their
        # definition differs. This will ensure a non-empty patch in the plan.
        loc_man = srv_man = {meta: make_manifest("Namespace", None, "ns1")}

        # Simulate an error in `partition_manifests`.
        m_part.return_value = (None, True)
        assert sq.compile_plan(config, k8sconfig, loc_man, srv_man) == err_resp

        # Simulate an error in `diff`.
        m_part.return_value = (plan, False)
        m_plan.return_value = (None, True)
        assert sq.compile_plan(config, k8sconfig, loc_man, srv_man) == err_resp

        # Simulate an error in `make_patch`.
        m_part.return_value = (plan, False)
        m_plan.return_value = ("some string", False)
        m_apply.return_value = (None, True)
        assert sq.compile_plan(config, k8sconfig, loc_man, srv_man) == err_resp

    def test_compile_plan_err_strip(self, config, k8sconfig):
        """Abort if any of the manifests cannot be stripped."""
        err_resp = (DeploymentPlan(tuple(), tuple(), tuple()), True)

        # Create two valid `ServerManifests`, then stunt one in such a way that
        # `manio.strip` will reject it.
        man_valid = make_manifest("Deployment", "namespace", "name")
        man_error = make_manifest("Deployment", "namespace", "name")
        meta_valid = manio.make_meta(man_valid)
        meta_error = manio.make_meta(man_error)

        # Stunt one manifest.
        del man_error["kind"]

        # Compile to `ServerManifest` types.
        valid = {meta_valid: man_valid}
        error = {meta_error: man_error}

        # Must handle errors from `manio.strip`.
        assert sq.compile_plan(config, k8sconfig, valid, error) == err_resp
        assert sq.compile_plan(config, k8sconfig, error, valid) == err_resp
        assert sq.compile_plan(config, k8sconfig, error, error) == err_resp


class TestMainOptions:
    @mock.patch.object(k8s, "post")
    @mock.patch.object(k8s, "patch")
    @mock.patch.object(k8s, "delete")
    def test_apply_plan(self, m_delete, m_apply, m_post, config, kube_creds):
        """Simulate a successful resource update (add, patch, delete).

        To this end, create a valid (mocked) deployment plan, mock out all
        calls, and verify that all the necessary calls are made.

        The second part of the test simulates errors. This is not a separate
        test because it shares virtually all the boiler plate code.
        """
        # Valid MetaManifest.
        meta = manio.make_meta(make_manifest("Deployment", "ns", "name"))

        # Valid Patch.
        patch = JsonPatch(
            url="patch_url",
            ops=[
                {'op': 'remove', 'path': '/metadata/labels/old'},
                {'op': 'add', 'path': '/metadata/labels/new', 'value': 'new'},
            ],
        )

        # Valid non-empty deployment plan.
        plan = DeploymentPlan(
            create=[DeltaCreate(meta, "create_url", "create_man")],
            patch=[DeltaPatch(meta, "diff", patch)],
            delete=[DeltaDelete(meta, "delete_url", "delete_man")],
        )

        def reset_mocks():
            m_post.reset_mock()
            m_apply.reset_mock()
            m_delete.reset_mock()

            # Pretend that all K8s requests succeed.
            m_post.return_value = (None, False)
            m_apply.return_value = (None, False)
            m_delete.return_value = (None, False)

        # Update the K8s resources and verify that the test functions made the
        # corresponding calls to K8s.
        reset_mocks()
        assert sq.apply_plan(config, plan) is False
        m_post.assert_called_once_with("k8s_client", "create_url", "create_man")
        m_apply.assert_called_once_with("k8s_client", patch.url, patch.ops)
        m_delete.assert_called_once_with("k8s_client", "delete_url", "delete_man")

        # -----------------------------------------------------------------
        #                   Simulate An Empty Plan
        # -----------------------------------------------------------------
        # Repeat the test and ensure the function does not even ask for
        # confirmation if the plan is empty.
        reset_mocks()
        empty_plan = DeploymentPlan(create=[], patch=[], delete=[])

        # Call test function and verify that it did not try to apply
        # the empty plan.
        assert sq.apply_plan(config, empty_plan) is False
        assert not m_post.called
        assert not m_apply.called
        assert not m_delete.called

        # -----------------------------------------------------------------
        #                   Simulate Error Scenarios
        # -----------------------------------------------------------------
        reset_mocks()

        # Make `delete` fail.
        m_delete.return_value = (None, True)
        assert sq.apply_plan(config, plan) is True

        # Make `patch` fail.
        m_apply.return_value = (None, True)
        assert sq.apply_plan(config, plan) is True

        # Make `post` fail.
        m_post.return_value = (None, True)
        assert sq.apply_plan(config, plan) is True

    @mock.patch.object(manio, "load")
    @mock.patch.object(manio, "download")
    @mock.patch.object(manio, "align_serviceaccount")
    @mock.patch.object(sq, "compile_plan")
    def test_make_plan(self, m_plan, m_align, m_down, m_load, config, kube_creds):
        """Basic test.

        This function does hardly anything to begin with, so we will merely
        verify it calls the correct functions with the correct arguments and
        handles errors correctly.

        """
        k8sconfig: K8sConfig = kube_creds
        err_resp = (DeploymentPlan(tuple(), tuple(), tuple()), True)

        # Valid deployment plan.
        plan = DeploymentPlan(create=[], patch=[], delete=[])

        # All auxiliary functions will succeed.
        m_load.return_value = ("local", None, False)
        m_down.return_value = ("server", False)
        m_plan.return_value = (plan, False)
        m_align.side_effect = lambda loc_man, srv_man: (loc_man, False)

        # A successful DIFF only computes and prints the plan.
        plan, err = sq.make_plan(config)
        assert not err and isinstance(plan, DeploymentPlan)
        m_load.assert_called_once_with(config.folder, config.selectors)
        m_down.assert_called_once_with(config, k8sconfig)
        m_plan.assert_called_once_with(config, k8sconfig, "local", "server")

        # Make `compile_plan` fail.
        m_plan.return_value = (None, True)
        assert sq.make_plan(config) == err_resp

        # Make `download_manifests` fail.
        m_down.return_value = (None, True)
        assert sq.make_plan(config) == err_resp

        # Make `load` fail.
        m_load.return_value = (None, None, True)
        assert sq.make_plan(config) == err_resp

    @mock.patch.object(manio, "load")
    @mock.patch.object(manio, "download")
    @mock.patch.object(sq, "match_api_version")
    @mock.patch.object(manio, "sync")
    @mock.patch.object(manio, "save")
    def test_get_resources(self, m_save, m_sync, m_mapi, m_down,
                           m_load, kube_creds, config):
        """Basic test.

        The `get_resource` function is more of a linear script than anything
        else. We merely need to verify it calls the correct functions with the
        correct arguments and aborts if any errors occur.

        """
        k8sconfig: K8sConfig = kube_creds

        # Simulate successful responses from the two auxiliary functions.
        # The `load` function must return empty dicts to ensure the error
        # conditions are properly coded.
        m_load.return_value = ("local_meta", "local_path", False)
        m_down.return_value = ("server", False)
        m_mapi.return_value = ("matched", False)
        m_sync.return_value = ("synced", False)
        m_save.return_value = False

        # `manio.load` must have been called with a wildcard selector to ensure
        # it loads _all_ resources from the local files, even if we want to
        # sync only a subset.
        load_selectors = Selectors(kinds=k8sconfig.kinds, labels=[], namespaces=[])

        # Call test function and verify it passed the correct arguments.
        assert sq.get_resources(config) is False
        m_load.assert_called_once_with(config.folder, load_selectors)
        m_down.assert_called_once_with(config, k8sconfig)
        m_mapi.assert_called_once_with(k8sconfig, "local_meta", "server")
        m_sync.assert_called_once_with("local_path", "matched",
                                       config.selectors, config.groupby)
        m_save.assert_called_once_with(config.folder, "synced", config.priorities)

        # Simulate an error with `manio.save`.
        m_save.return_value = (None, True)
        assert sq.get_resources(config) is True

        # Simulate an error with `manio.sync`.
        m_sync.return_value = (None, True)
        assert sq.get_resources(config) is True

        # Simulate an error in `download_manifests`.
        m_down.return_value = (None, True)
        assert sq.get_resources(config) is True

        # Simulate an error in `load`.
        m_load.return_value = (None, None, True)
        assert sq.get_resources(config) is True
