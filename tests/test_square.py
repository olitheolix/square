import copy
import logging
import random
import unittest.mock as mock
from pathlib import Path
from typing import cast

import pytest
import yaml

import square.cfgfile
import square.k8s as k8s
import square.manio
import square.manio as manio
import square.square as sq
from square.dtypes import (
    DEFAULT_PRIORITIES, Config, DeltaCreate, DeltaDelete, DeltaPatch,
    DeploymentPlan, DeploymentPlanMeta, GroupBy, JsonPatch, K8sConfig,
    KindName, MetaManifest, Selectors, SquareManifests,
)
from square.k8s import resource

from .test_helpers import make_manifest


class TestLogging:
    def test_setup_logging(self):
        """Ensure the function runs and is idempotent."""
        # The `logging` module will automatically install a default handler.
        logger = logging.getLogger("square")
        assert len(logger.handlers) == 1

        # Test function must accept all log levels. It must also be idempotent
        # and calling it multiple times must not add more handlers but replace
        # the existing one.
        for level in range(10):
            sq.setup_logging(level)
            assert len(logger.handlers) == 1


class TestBasic:
    def test_config_default(self, tmp_path):
        """Default values for Config."""
        assert Config(folder=tmp_path, kubeconfig=tmp_path, kubecontext="ctx") == Config(
            folder=tmp_path,
            kubecontext="ctx",
            kubeconfig=tmp_path,
            selectors=Selectors(),
            priorities=list(DEFAULT_PRIORITIES),
            groupby=GroupBy(),
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
            create=[DeltaCreate(meta, "url", {})],
            patch=[
                DeltaPatch(meta, "", patch),
                DeltaPatch(meta, "  normal\n+  add\n-  remove", patch)
            ],
            delete=[DeltaDelete(meta, "url", {})],
        )
        assert sq.show_plan(plan) is False

    def test_translate_resource_kinds_simple(self, k8sconfig):
        """Translate various spellings in `selectors.kinds`"""
        cfg = Config(
            folder=Path('/tmp'),
            kubeconfig=Path(),
            kubecontext=None,
            groupby=GroupBy(),
            priorities=["ns", "DEPLOYMENT"],
            selectors=Selectors(kinds={"svc", 'DEPLOYMENT', "Secret"}),
        )

        # Convert the resource names to their correct K8s kind.
        ret = sq.translate_resource_kinds(cfg, k8sconfig)
        assert ret.priorities == ["Namespace", "Deployment"]
        assert ret.selectors.kinds == {"Service", "Deployment", "Secret"}
        assert ret.selectors._kinds_only == {"Service", "Deployment", "Secret"}
        assert ret.selectors._kinds_names == [
            KindName(kind="Deployment", name=""),
            KindName(kind="Secret", name=""),
            KindName(kind="Service", name=""),
        ]

        # Add two invalid resource names. This must succeed but return the
        # resource names without having changed them.
        cfg.selectors.kinds.clear()
        cfg.selectors.kinds.update({"invalid", "k8s-resource-kind"})
        cfg.priorities.clear()
        cfg.priorities.extend(["invalid", "k8s-resource-kind"])
        ret = sq.translate_resource_kinds(cfg, k8sconfig)
        assert ret.priorities == ["invalid", "k8s-resource-kind"]
        assert ret.selectors.kinds == {"invalid", "k8s-resource-kind"}
        assert ret.selectors._kinds_only == {"invalid", "k8s-resource-kind"}
        assert ret.selectors._kinds_names == [
            KindName(kind="invalid", name=""),
            KindName(kind="k8s-resource-kind", name=""),
        ]

    def test_translate_resource_kinds_kind_name(self, k8sconfig):
        """Same as previous test but this time also specify `kind/name`."""
        kinds = {"svc/app1", "DEPLOYMENT/app2",
                 "ns", "namespace/foo",
                 "unknown-a", "unknown-b/foo"}
        cfg = Config(
            folder=Path('/tmp'),
            kubeconfig=Path(),
            kubecontext=None,
            groupby=GroupBy(),
            priorities=[],
            selectors=Selectors(kinds=kinds),
        )

        # Verify the baseline.
        assert cfg.selectors.kinds == kinds
        assert cfg.selectors._kinds_only == {"ns", "unknown-a"}
        assert cfg.selectors._kinds_names == [
            KindName(kind="DEPLOYMENT", name="app2"),
            KindName(kind="namespace", name="foo"),
            KindName(kind="ns", name=""),
            KindName(kind="svc", name="app1"),
            KindName(kind="unknown-a", name=""),
            KindName(kind="unknown-b", name="foo"),
        ]

        # Convert the selector KINDs to their canonical K8s KINDs.
        ret = sq.translate_resource_kinds(cfg, k8sconfig)
        assert ret.selectors.kinds == {
            "Service/app1", "Deployment/app2",
            "Namespace", "Namespace/foo",
            "unknown-a", "unknown-b/foo"
        }
        assert ret.selectors._kinds_only == {"Namespace", "unknown-a"}
        assert ret.selectors._kinds_names == [
            KindName(kind="Deployment", name="app2"),
            KindName(kind="Namespace", name=""),
            KindName(kind="Namespace", name="foo"),
            KindName(kind="Service", name="app1"),
            KindName(kind="unknown-a", name=""),
            KindName(kind="unknown-b", name="foo"),
        ]

    def test_valid_label(self):
        """Test label values (not their key names)."""
        # A specific example of a valid label that triggered a bug once.
        assert sq.valid_label("dh/repo=pd-devops-charts")

        # Valid specimen.
        valid = ["foo", "foo/bar", "f/oo", "tags.datadoghq.com/service"]

        # Invalid specimen.
        invalid = [
            "fo o/valid", "f*o/valid", "foo-/valid", "-foo/valid",
            ".foo/valid", "foo./valid", "_foo/valid", "foo_/valid",
            "./valid", "-/valid", "_/valid", "b ar/valid",
            "foo /bar", "foo/ bar", "foo / bar", "foo/bar/foobar",
            "foo//bar", "/bar", "foo/", "tags.datadoghq.com/",
        ]

        valid = [f"{_}=value" for _ in valid]
        invalid = [f"{_}=value" for _ in invalid]

        for name in valid:
            assert sq.valid_label(name)

        for name in invalid:
            assert not sq.valid_label(name)

    async def test_sanity_check_labels(self, config):
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
            assert await sq.get_resources(config) is True
            assert await sq.apply_plan(config, plan) is True
            assert await sq.make_plan(config) == (plan, True)


class TestPartition:
    def test_partition_manifests_patch(self):
        """Local and server manifests match.

        If all resources exist both locally and remotely then nothing needs to
        be created or deleted. However, the resources may need patching but
        that is not something `partition_manifests` concerns itself with.

        """
        # Local and cluster manifests are identical - the Plan must not
        # create/add anything but mark all resources for (possible)
        # patching.
        manifests: SquareManifests = {
            MetaManifest('v1', 'Namespace', None, 'ns3'): {},
            MetaManifest('v1', 'Namespace', None, 'ns1'): {},
            MetaManifest('v1', 'Deployment', 'ns2', 'bar'): {},
            MetaManifest('v1', 'Namespace', None, 'ns2'): {},
            MetaManifest('v1', 'Deployment', 'ns1', 'foo'): {},
        }
        plan = DeploymentPlanMeta(create=[], patch=list(manifests.keys()), delete=[])
        assert sq.partition_manifests(manifests, manifests) == (plan, False)

    def test_partition_manifests_add_delete(self):
        """Local and server manifests are orthogonal sets.

        This must produce a plan where all local resources will be created, all
        cluster resources deleted and none patched.

        """
        fun = sq.partition_manifests

        # Local and cluster manifests are orthogonal.
        local_man: SquareManifests = {
            MetaManifest('v1', 'Deployment', 'ns2', 'bar'): {},
            MetaManifest('v1', 'Namespace', None, 'ns2'): {},
        }
        cluster_man: SquareManifests = {
            MetaManifest('v1', 'Deployment', 'ns1', 'foo'): {},
            MetaManifest('v1', 'Namespace', None, 'ns1'): {},
            MetaManifest('v1', 'Namespace', None, 'ns3'): {},
        }
        plan = DeploymentPlanMeta(
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
        local_man: SquareManifests = {
            MetaManifest('v1', 'Deployment', 'ns2', 'bar1'): {},
            MetaManifest('v1', 'Namespace', None, 'ns2'): {},
        }
        cluster_man: SquareManifests = {
            MetaManifest('v1', 'Deployment', 'ns1', 'foo'): {},
            MetaManifest('v1', 'Deployment', 'ns2', 'bar1'): {},
            MetaManifest('v1', 'Deployment', 'ns2', 'bar2'): {},
            MetaManifest('v1', 'Namespace', None, 'ns1'): {},
            MetaManifest('v1', 'Namespace', None, 'ns2'): {},
            MetaManifest('v1', 'Namespace', None, 'ns3'): {},
        }
        plan = DeploymentPlanMeta(
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
    def test_make_patch_empty(self, k8sconfig):
        """Basic test: compute patch between two identical resources."""
        # Setup.
        kind, ns, name = 'Deployment', 'ns', 'foo'

        # PATCH URLs require the resource name at the end of the request path.
        url = resource(k8sconfig, MetaManifest("apps/v1", kind, ns, name))[0].url

        # The patch must be empty for identical manifests.
        loc = srv = make_manifest(kind, ns, name)
        data, err = sq.make_patch(k8sconfig, loc, srv)
        assert (data, err) == (JsonPatch(url, []), False)
        assert isinstance(data, JsonPatch)

    def test_make_patch_incompatible(self, k8sconfig):
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
        assert sq.make_patch(k8sconfig, loc, srv) == err_resp

        # `kind` must match.
        loc = copy.deepcopy(srv)
        loc['kind'] = 'Mismatch'
        assert sq.make_patch(k8sconfig, loc, srv) == err_resp

        # `name` must match.
        loc = copy.deepcopy(srv)
        loc['metadata']['name'] = 'mismatch'
        assert sq.make_patch(k8sconfig, loc, srv) == err_resp

        # `namespace` must match.
        loc = copy.deepcopy(srv)
        loc['metadata']['namespace'] = 'mismatch'
        assert sq.make_patch(k8sconfig, loc, srv) == err_resp

    def test_make_patch_special(self, k8sconfig):
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
            assert sq.make_patch(k8sconfig, loc, srv) == ((url, []), False)

            # Create two almost identical manifests, except the second one has
            # different `metadata.labels`. This must succeed.
            loc = make_manifest(kind, None, "name")
            srv = copy.deepcopy(loc)
            loc['metadata']['labels'] = {"key": "value"}

            data, err = sq.make_patch(k8sconfig, loc, srv)
            assert err is False and len(data) > 0

    @mock.patch.object(k8s, "resource")
    def test_make_patch_error_resource(self, m_url, k8sconfig):
        """Coverage gap: simulate `resource` error."""
        # Simulate `resource` error.
        m_url.return_value = (None, True)

        # Test function must return with error.
        loc = srv = make_manifest("Deployment", "ns", "foo")
        assert sq.make_patch(k8sconfig, loc, srv) == (JsonPatch("", []), True)


class TestMatchApiVersions:
    @mock.patch.object(k8s, "get")
    async def test_match_api_version_basic(self, m_get, k8sconfig):
        """Square must use the API version declared in local manifests.

        In this case, we have an HPA resource. The local manifest uses v1
        whereas K8s will have automatically converted it to the latest version,
        which happens to be v2 for K8s 1.26.

        This test verifies that Square requests the manifest from the `v1`
        endpoint as specified in the manifest, even though K8s stores it in
        `v2` format.

        """
        hpa = "HorizontalPodAutoscaler"

        # Create local and server manifests. Both specify the same HPA resource
        # but with different `apiVersions`.
        man_hpa_loc = make_manifest(hpa, "ns", "name")
        man_hpa_srv_v1 = make_manifest(hpa, "ns", "name")
        man_hpa_srv_v2 = make_manifest(hpa, "ns", "name")
        man_hpa_loc.update(dict(apiVersion="autoscaling/v1", src="loc"))
        man_hpa_srv_v1.update(dict(apiVersion="autoscaling/v1", src="srv"))
        man_hpa_srv_v2.update(dict(apiVersion="autoscaling/v2", src="srv"))
        meta_hpa_loc = manio.make_meta(man_hpa_loc)
        meta_hpa_srv_v1 = manio.make_meta(man_hpa_srv_v1)
        meta_hpa_srv_v2 = manio.make_meta(man_hpa_srv_v2)
        local: SquareManifests = {meta_hpa_loc: man_hpa_loc}
        server: SquareManifests = {meta_hpa_srv_v2: man_hpa_srv_v2}

        # Mock the resource download. If the function works correctly it will
        # have fetched it from the `autoscaling/v1` endpoint as specified in
        # the local manifest. We will verify that later.
        m_get.return_value = (man_hpa_srv_v1, False)

        # Test function must have interrogated the `autoscaling/v1` as
        # specified in the *local* manifest, even though K8s serves
        # it from `autoscaling/v2` by default.
        srv, err = await square.square.match_api_version(k8sconfig, local, server)
        assert not err and srv == {meta_hpa_srv_v1: man_hpa_srv_v1}

        # Must have downloaded the HPAs from the `autoscaling/v1` endpoint
        # because that is what the local manifest specified.
        resource, err = k8s.resource(k8sconfig, meta_hpa_loc)
        assert not err
        m_get.assert_called_once_with(k8sconfig, resource.url)

    @mock.patch.object(k8s, "get")
    async def test_match_api_version_namespace(self, m_get, k8sconfig):
        """Square must use the API version declared in local manifests.

        This is the same as `test_match_api_version_basic` except it operates
        on a list of two resources in two different namespaces.

        """
        hpa = "HorizontalPodAutoscaler"

        # Create local and server manifests. Both specify the same HPAs
        # but K8s and the local manifests use different `apiVersions`.
        man_hpa_ns1_loc = make_manifest(hpa, "ns1", "name-1")
        man_hpa_ns2_loc = make_manifest(hpa, "ns2", "name-2")
        man_hpa_ns1_srv = make_manifest(hpa, "ns1", "name-1")
        man_hpa_ns2_srv_v1 = make_manifest(hpa, "ns2", "name-2")
        man_hpa_ns2_srv_v2 = make_manifest(hpa, "ns2", "name-2")

        # All but the local HPA in NS1 are from the v2 endpoint.
        man_hpa_ns1_loc.update(dict(apiVersion="autoscaling/v2", src="loc"))
        man_hpa_ns2_loc.update(dict(apiVersion="autoscaling/v1", src="loc"))
        man_hpa_ns1_srv.update(dict(apiVersion="autoscaling/v2", src="srv"))
        man_hpa_ns2_srv_v1.update(dict(apiVersion="autoscaling/v1", src="srv"))
        man_hpa_ns2_srv_v2.update(dict(apiVersion="autoscaling/v2", src="srv"))

        meta_hpa_ns1_loc = manio.make_meta(man_hpa_ns1_loc)
        meta_hpa_ns1_srv = manio.make_meta(man_hpa_ns1_srv)
        meta_hpa_ns2_loc = manio.make_meta(man_hpa_ns2_loc)
        meta_hpa_ns2_srv_v1 = manio.make_meta(man_hpa_ns2_srv_v1)
        meta_hpa_ns2_srv_v2 = manio.make_meta(man_hpa_ns2_srv_v2)
        local: SquareManifests = {
            meta_hpa_ns1_loc: man_hpa_ns1_loc,
            meta_hpa_ns2_loc: man_hpa_ns2_loc,
        }
        server: SquareManifests = {
            meta_hpa_ns1_srv: man_hpa_ns1_srv,
            meta_hpa_ns2_srv_v2: man_hpa_ns2_srv_v2,
        }

        # Mock the resource download to supply it from the correct API endpoint.
        m_get.return_value = (man_hpa_ns2_srv_v1, False)

        # Test function must return the updated manifests. Here we verify that
        # it does indeed return the new `server` manifests. We will verify that
        # those came from the correct endpoint afterwards.
        srv, err = await square.square.match_api_version(k8sconfig, local, server)
        assert not err and srv == {
            meta_hpa_ns1_srv: man_hpa_ns1_srv,
            meta_hpa_ns2_srv_v1: man_hpa_ns2_srv_v1,
        }

        # Test function must have made only one call because only one HPA
        # specified a different endpoint than the server.
        resource, err = k8s.resource(k8sconfig, meta_hpa_ns2_loc)
        assert not err
        m_get.assert_called_once_with(k8sconfig, resource.url)

    @mock.patch.object(k8s, "get")
    async def test_match_api_version_multi(self, m_get, k8sconfig):
        """Mix matching and mis-matching API version for same resources.

        A trivial extension to `test_match_api_version_namespace` where we use
        three HPA resources from the same namespace. This test ensures that
        Square inspects the resources one-by-one instead of applying the API
        version rule by namespace.

        """
        hpa = "HorizontalPodAutoscaler"

        # Create local and server manifests. The second local HPA uses the
        # old `autoscaling/v1` endpoint, whereas all other use `v2`.
        man_hpa_1_loc = make_manifest(hpa, "ns", "name-1")
        man_hpa_2_loc = make_manifest(hpa, "ns", "name-2")
        man_hpa_3_loc = make_manifest(hpa, "ns", "name-3")
        man_hpa_1_srv = make_manifest(hpa, "ns", "name-1")
        man_hpa_2_srv_v1 = make_manifest(hpa, "ns", "name-2")
        man_hpa_2_srv_v2 = make_manifest(hpa, "ns", "name-2")
        man_hpa_3_srv = make_manifest(hpa, "ns", "name-3")

        man_hpa_1_loc.update(dict(apiVersion="autoscaling/v2", src="loc"))
        man_hpa_2_loc.update(dict(apiVersion="autoscaling/v1", src="loc"))
        man_hpa_3_loc.update(dict(apiVersion="autoscaling/v2", src="loc"))
        man_hpa_1_srv.update(dict(apiVersion="autoscaling/v2", src="srv"))
        man_hpa_2_srv_v1.update(dict(apiVersion="autoscaling/v1", src="srv"))
        man_hpa_2_srv_v2.update(dict(apiVersion="autoscaling/v2", src="srv"))
        man_hpa_3_srv.update(dict(apiVersion="autoscaling/v2", src="srv"))

        meta_hpa_1_loc = manio.make_meta(man_hpa_1_loc)
        meta_hpa_2_loc = manio.make_meta(man_hpa_2_loc)
        meta_hpa_3_loc = manio.make_meta(man_hpa_3_srv)
        meta_hpa_1_srv = manio.make_meta(man_hpa_1_srv)
        meta_hpa_2_srv_v1 = manio.make_meta(man_hpa_2_srv_v1)
        meta_hpa_2_srv_v2 = manio.make_meta(man_hpa_2_srv_v2)
        meta_hpa_3_srv = manio.make_meta(man_hpa_3_srv)

        local: SquareManifests = {
            meta_hpa_1_loc: man_hpa_1_loc,
            meta_hpa_2_loc: man_hpa_2_loc,
            meta_hpa_3_loc: man_hpa_3_loc,
        }
        server: SquareManifests = {
            meta_hpa_1_srv: man_hpa_1_srv,
            meta_hpa_2_srv_v2: man_hpa_2_srv_v2,
            meta_hpa_3_srv: man_hpa_3_srv,
        }

        # Mock the resource download to supply it from the correct API endpoint.
        m_get.return_value = (man_hpa_2_srv_v1, False)

        # Test function must return the updated manifests. Here we verify that
        # it does indeed return the new `server` manifests. We will verify that
        # those came from the correct endpoint afterwards.
        srv, err = await square.square.match_api_version(k8sconfig, local, server)
        assert not err and srv == {
            meta_hpa_1_srv: man_hpa_1_srv,
            meta_hpa_2_srv_v1: man_hpa_2_srv_v1,

            # This must have been downloaded.
            meta_hpa_3_srv: man_hpa_3_srv,
        }

        # Test function must have made only one call because only one HPA
        # specified a different endpoint than the server.
        resource, err = k8s.resource(k8sconfig, meta_hpa_2_loc)
        assert not err
        m_get.assert_called_once_with(k8sconfig, resource.url)

    @mock.patch.object(k8s, "get")
    async def test_match_api_version_nothing_to_do(self, m_get, k8sconfig):
        """Test various cases where the function must not do anything.

        There are two cases where it must not re-download a resource from K8s:
          1) Local/Server manifests all use the same API endpoint.
          2) Resource exists either only on the server or locally, but not both.

        """
        fun = square.square.match_api_version
        hpa = "HorizontalPodAutoscaler"
        svc = "Service"

        man_svc_1_loc = make_manifest(svc, "ns", "svc-1")
        man_svc_1_srv = make_manifest(svc, "ns", "svc-1")
        man_hpa_1_loc = make_manifest(hpa, "ns", "hpa-1")
        man_hpa_1_srv = make_manifest(hpa, "ns", "name-1")
        man_hpa_2_srv = make_manifest(hpa, "ns", "name-2")

        man_svc_1_loc.update(dict(apiVersion="v1", src="loc"))
        man_svc_1_srv.update(dict(apiVersion="v1", src="srv"))
        man_hpa_1_loc.update(dict(apiVersion="autoscaling/v1", src="loc"))
        man_hpa_1_srv.update(dict(apiVersion="autoscaling/v2", src="srv"))
        man_hpa_2_srv.update(dict(apiVersion="autoscaling/v2", src="srv"))

        meta_svc_1_loc = manio.make_meta(man_svc_1_loc)
        meta_svc_1_srv = manio.make_meta(man_svc_1_srv)
        meta_hpa_1_loc = manio.make_meta(man_hpa_1_loc)
        meta_hpa_2_srv = manio.make_meta(man_hpa_2_srv)

        # Must not have downloaded anything.
        srv, err = await fun(k8sconfig, {}, {})
        assert not err and srv == {}
        assert not m_get.called

        # Local and server manifests are identical - must not synchronise anything.
        local: SquareManifests = {
            meta_svc_1_loc: man_svc_1_loc,
            meta_hpa_1_loc: man_hpa_1_loc,
        }
        srv, err = await fun(k8sconfig, local, local)
        assert not err and srv == local
        assert not m_get.called

        # Local- and server manifests have identical Service resources but use
        # two different API endpoints for two different HPAs. Must not
        # sync any API versions since the HPAs are unrelated.
        local: SquareManifests = {
            meta_svc_1_loc: man_svc_1_loc,
            meta_hpa_1_loc: man_hpa_1_loc,
        }
        server: SquareManifests = {
            meta_svc_1_srv: man_svc_1_srv,
            meta_hpa_2_srv: man_hpa_2_srv,
        }
        srv, err = await fun(k8sconfig, local, server)
        assert not err and srv == server
        assert not m_get.called

        # Local- and server manifests have matching Deployments in two
        # namespaces. Function must therefore not match anything.
        local: SquareManifests = {
            meta_svc_1_loc: man_svc_1_loc,
            meta_hpa_1_loc: man_hpa_1_loc,
        }
        server: SquareManifests = {
            meta_svc_1_loc: man_svc_1_srv,
            meta_hpa_1_loc: man_hpa_1_srv,
        }
        srv, err = await fun(k8sconfig, local, server)
        assert not err and srv == server
        assert not m_get.called


class TestPlan:
    def test_make_patch_ok(self, k8sconfig):
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
        assert sq.make_patch(k8sconfig, loc, loc) == (expected, False)

        # The patch between `srv` and `loc` must remove the old label and add
        # the new one.
        expected = JsonPatch(
            url=res.url,
            ops=[
                {'op': 'remove', 'path': '/metadata/labels/old'},
                {'op': 'add', 'path': '/metadata/labels/new', 'value': 'new'}
            ]
        )
        assert sq.make_patch(k8sconfig, loc, srv) == (expected, False)

    def test_make_patch_err(self, k8sconfig):
        """Verify error cases with invalid or incompatible manifests."""
        err_resp = (JsonPatch("", []), True)

        kind, namespace, name = "Deployment", "namespace", "name"
        valid = make_manifest(kind, namespace, name)

        # Must handle `resource` errors.
        with mock.patch.object(sq.k8s, "resource") as m_url:
            m_url.return_value = (None, True)
            assert sq.make_patch(k8sconfig, valid, valid) == err_resp

        # Must handle incompatible manifests, ie manifests that do not belong
        # to the same resource.
        valid_a = make_manifest(kind, namespace, "bar")
        valid_b = make_manifest(kind, namespace, "foo")
        assert sq.make_patch(k8sconfig, valid_a, valid_b) == err_resp

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
                DeltaCreate(meta_ns0, "url", {}),
                DeltaCreate(meta_ns1, "url", {}),
                DeltaCreate(meta_svc0, "url", {}),
                DeltaCreate(meta_svc1, "url", {}),
                DeltaCreate(meta_dpl0, "url", {}),
                DeltaCreate(meta_dpl1, "url", {}),
            ],
            patch=[
                DeltaPatch(meta_ns0, "url", JsonPatch("url", [{}])),
                DeltaPatch(meta_ns1, "url", JsonPatch("url", [{}])),
                DeltaPatch(meta_svc0, "url", JsonPatch("url", [{}])),
                DeltaPatch(meta_svc1, "url", JsonPatch("url", [{}])),
                DeltaPatch(meta_dpl0, "url", JsonPatch("url", [{}])),
                DeltaPatch(meta_dpl1, "url", JsonPatch("url", [{}])),
            ],
            delete=[
                DeltaDelete(meta_dpl1, "url", {}),
                DeltaDelete(meta_dpl0, "url", {}),
                DeltaDelete(meta_svc1, "url", {}),
                DeltaDelete(meta_svc0, "url", {}),
                DeltaDelete(meta_ns1, "url", {}),
                DeltaDelete(meta_ns0, "url", {}),
            ],
        )

        config.priorities = ["Namespace", "Service", "Deployment"]
        plan = copy.deepcopy(expected)
        for _ in range(10):
            random.shuffle(cast(list, plan.create))
            random.shuffle(cast(list, plan.patch))
            random.shuffle(cast(list, plan.delete))
            ret, err = sq.sort_plan(config, plan)
            assert not err
            assert ret.create == expected.create
            assert ret.delete == expected.delete
            assert ret.patch == plan.patch

        # Service must be last because it is not in the priority list.
        expected = DeploymentPlan(
            create=[
                DeltaCreate(meta_ns0, "url", {}),
                DeltaCreate(meta_ns1, "url", {}),
                DeltaCreate(meta_dpl0, "url", {}),
                DeltaCreate(meta_svc1, "url", {}),
            ],
            patch=[
                DeltaPatch(meta_ns0, "", JsonPatch("", [{}])),
                DeltaPatch(meta_ns1, "", JsonPatch("", [{}])),
                DeltaPatch(meta_svc0, "", JsonPatch("", [{}])),
                DeltaPatch(meta_svc1, "", JsonPatch("", [{}])),
                DeltaPatch(meta_dpl0, "", JsonPatch("", [{}])),
                DeltaPatch(meta_dpl1, "", JsonPatch("", [{}])),
            ],
            delete=[
                DeltaDelete(meta_svc0, "url", {}),
                DeltaDelete(meta_dpl1, "url", {}),
                DeltaDelete(meta_ns1, "url", {}),
                DeltaDelete(meta_ns0, "url", {}),
            ],
        )
        config.priorities = ["Namespace", "Deployment"]
        plan = copy.deepcopy(expected)
        for _ in range(10):
            random.shuffle(cast(list, plan.create))
            random.shuffle(cast(list, plan.patch))
            random.shuffle(cast(list, plan.delete))
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

    async def test_compile_plan_create_delete_ok(self, config, k8sconfig):
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
                DeltaDelete(meta[2], res[2].url + "/" + str(meta[2].name), del_opts),
                DeltaDelete(meta[3], res[3].url + "/" + str(meta[3].name), del_opts),
                DeltaDelete(meta[4], res[4].url + "/" + str(meta[4].name), del_opts),
            ],
        )
        ret, err = await sq.compile_plan(config, k8sconfig, loc_man, srv_man)
        assert ret.create == expected.create
        assert (ret, err) == (expected, False)

    @mock.patch.object(sq, "match_api_version")
    async def test_compile_plan_create_delete_err(self, m_part, config, k8sconfig):
        """Simulate `resource` errors."""
        err_resp = (DeploymentPlan(tuple(), tuple(), tuple()), True)

        # Valid ManifestMeta and dummy manifest dict.
        man = make_manifest("Deployment", "ns", "name")
        meta = manio.make_meta(man)
        man = {meta: man}

        # Pretend we only have to "create" resources and then trigger the
        # `resource` error in its code path.
        m_part.return_value = (
            DeploymentPlan(create=[DeltaCreate(meta, "url", {})], patch=[], delete=[]),
            False
        )

        # We must not be able to compile a plan because of the `resource` error.
        with mock.patch.object(sq.k8s, "resource") as m_url:
            m_url.return_value = (None, True)
            assert await sq.compile_plan(config, k8sconfig, man, man) == err_resp

        # Pretend we only have to "delete" resources, and then trigger the
        # `resource` error in its code path.
        m_part.return_value = (
            DeploymentPlan(create=[], patch=[], delete=[DeltaDelete(meta, "url", {})]),
            False
        )
        with mock.patch.object(sq.k8s, "resource") as m_url:
            m_url.return_value = (None, True)
            assert await sq.compile_plan(config, k8sconfig, man, man) == err_resp

    async def test_compile_plan_patch_no_diff(self, config, k8sconfig):
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
        assert await sq.compile_plan(config, k8sconfig, src, src) == (expected, False)

    async def test_compile_plan_invalid_api_version(self, config, k8sconfig):
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
        ret = await sq.compile_plan(config, k8sconfig, src, src)
        assert ret == (DeploymentPlan(tuple(), tuple(), tuple()), True)

    async def test_compile_plan_patch_with_diff(self, config, k8sconfig):
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
        patch, err = sq.make_patch(k8sconfig, loc_man[meta], srv_man[meta])
        assert not err
        diff_str, err = manio.diff(loc_man[meta], srv_man[meta])
        assert not err

        # Verify the test function returns the correct Patch and diff.
        expected = DeploymentPlan(
            create=[],
            patch=[DeltaPatch(meta, diff_str, patch)],
            delete=[]
        )
        ret = await sq.compile_plan(config, k8sconfig, loc_man, srv_man)
        assert ret == (expected, False)

    @mock.patch.object(sq, "partition_manifests")
    @mock.patch.object(manio, "diff")
    @mock.patch.object(sq, "make_patch")
    async def test_compile_plan_err(self, m_apply, m_plan, m_part, config, k8sconfig):
        """Use mocks for the internal function calls to simulate errors."""
        err_resp = (DeploymentPlan(tuple(), tuple(), tuple()), True)

        # Define a single resource and valid dummy return value for
        # `sq.partition_manifests`.
        meta = MetaManifest('v1', 'Namespace', None, 'ns1')
        plan = DeploymentPlanMeta(create=[], patch=[meta], delete=[])

        # Local and server manifests have the same resources but their
        # definition differs. This will ensure a non-empty patch in the plan.
        loc_man = srv_man = {meta: make_manifest("Namespace", None, "ns1")}

        # Simulate an error in `partition_manifests`.
        m_part.return_value = (None, True)
        assert await sq.compile_plan(config, k8sconfig, loc_man, srv_man) == err_resp

        # Simulate an error in `diff`.
        m_part.return_value = (plan, False)
        m_plan.return_value = (None, True)
        assert await sq.compile_plan(config, k8sconfig, loc_man, srv_man) == err_resp

        # Simulate an error in `make_patch`.
        m_part.return_value = (plan, False)
        m_plan.return_value = ("some string", False)
        m_apply.return_value = (None, True)
        assert await sq.compile_plan(config, k8sconfig, loc_man, srv_man) == err_resp

    async def test_compile_plan_err_strip(self, config, k8sconfig):
        """Abort if a manifests cannot be stripped.

        To facilitate this test we will install a `strip` callback that
        corrupts the manifest which will make `strip_manifests` return an
        error. The test function must then gracefully abort.

        """
        err_resp = (DeploymentPlan(tuple(), tuple(), tuple()), True)

        # Install a `strip` callback that corrupts `MetaManifest` info.
        def cb_corrupt(_cfg: Config, _man: dict):
            del _man["kind"]
            return _man

        config.strip_callback = cb_corrupt

        man = make_manifest("Deployment", "namespace", "name")
        meta = manio.make_meta(man)
        valid = {meta: man}

        # Must handle errors from `manio.strip_manifests`.
        assert await sq.compile_plan(config, k8sconfig, valid, valid) == err_resp

    def test_call_external_function(self):
        """Test various scenarios when calling a user supplied function."""
        def cb(data: str):
            assert data == "foo"
            return ("it", "worked")

        fun = sq.call_external_function

        # Well behaved.
        assert fun(cb, data="foo") == (("it", "worked"), False)

        # Function will raise an exception.
        assert fun(cb, dict(data="bar")) == (None, True)

        # Wrong call signature.
        assert fun(cb, {}) == (None, True)
        assert fun(cb, dict(invalid="bar")) == (None, True)

    def test_run_patch_callback(self, config):
        """Safeguard the call to the user supplied callback function.

        This test will define a few callback functions. Some will work as
        intended whereas others will raise exceptions. The goal is to ensure
        that `run_user_callback` can gracefully abort if the user supplied
        callback function misbehaves, for instance raise an exception or return
        the wrong number of arguments.

        """
        # Define a single resource.
        meta = MetaManifest('v1', 'Namespace', None, 'ns1')

        def get_dummy_manifests():
            """Return a valid local/server manfiest pair to force a patch."""
            loc_man = {meta: make_manifest("Namespace", None, "ns1")}
            srv_man = {meta: make_manifest("Namespace", None, "ns1")}
            loc_man[meta]["metadata"]["labels"] = {"foo": "foo"}
            srv_man[meta]["metadata"]["labels"] = {"bar": "bar"}
            return loc_man, srv_man

        # ----------------------------------------------------------------------
        # No custom callback function installed.
        # ----------------------------------------------------------------------
        local, server = get_dummy_manifests()
        assert not sq.run_patch_callback(config, [meta], local, server)
        assert local[meta] != server[meta]
        del local, server

        # ----------------------------------------------------------------------
        # Callback is well behaved: local manifest & server manifests match.
        # ----------------------------------------------------------------------
        local, server = get_dummy_manifests()

        def cb1(_cfg: Config, _local: dict, _server: dict):
            return (_server, _server)

        config.patch_callback = cb1
        assert not sq.run_patch_callback(config, [meta], local, server)
        assert local[meta] == server[meta]
        del cb1, local, server

        # ----------------------------------------------------------------------
        # Callback returns wrong number or arguments.
        # ----------------------------------------------------------------------
        def cb2(_cfg: Config, _local: dict, _server: dict):
            return None

        local, server = get_dummy_manifests()
        config.patch_callback = cb2
        assert sq.run_patch_callback(config, [meta], local, server)
        del cb2, local, server

        # ----------------------------------------------------------------------
        # Callback modifies a field that changes the `MetaManifest`.
        # ----------------------------------------------------------------------
        def cb3(_cfg: Config, _local: dict, _server: dict):
            _local["kind"] = "foo"
            _server["kind"] = "foo"
            return (_local, _server)

        local, server = get_dummy_manifests()
        config.patch_callback = cb3
        assert sq.run_patch_callback(config, [meta], local, server)
        del cb3, local, server

        # ----------------------------------------------------------------------
        # Callback deletes a necessary field for the `MetaManifest`.
        # ----------------------------------------------------------------------
        def cb4(_cfg: Config, _local: dict, _server: dict):
            del _local["kind"]
            del _server["metadata"]
            return (_local, _server)

        local, server = get_dummy_manifests()
        config.patch_callback = cb4
        assert sq.run_patch_callback(config, [meta], local, server)
        del cb4, local, server

    async def test_compile_plan_patch_callback(self, config, k8sconfig):
        """Test a plan that uses a custom callback for patches.

        The client and server have the same resource but one requires a patch.
        Here we ensure that this resource passes through the callback function
        before the patch is computed.

        """
        # Define a single resource.
        meta = MetaManifest('v1', 'Namespace', None, 'ns1')

        # Local and server manifests have the same resources but their
        # definition differs. This will ensure a non-empty patch in the plan.
        loc_man = {meta: make_manifest("Namespace", None, "ns1")}
        srv_man = {meta: make_manifest("Namespace", None, "ns1")}
        loc_man[meta]["metadata"]["labels"] = {"foo": "foo"}
        srv_man[meta]["metadata"]["labels"] = {"bar": "bar"}
        loc_man_bak = copy.deepcopy(loc_man)
        srv_man_bak = copy.deepcopy(srv_man)

        def cb1(square_config: Config, local_manifest: dict, server_manifest: dict):
            assert square_config == config
            assert local_manifest == loc_man[meta]
            assert server_manifest == srv_man[meta]
            return server_manifest, server_manifest

        # Create the plan with the default patch callback. This must produce a patch
        # because there is a difference between the local and remote manifests.
        ret, err = await sq.compile_plan(config, k8sconfig, loc_man, srv_man)
        assert not err and ret.create == ret.delete == [] and len(ret.patch) == 1

        # Repeat the test with a callback function. Our test CB will match
        # the local and server manifests and the plan must thus be empty.
        config.patch_callback = cb1
        ret = await sq.compile_plan(config, k8sconfig, loc_man, srv_man)
        assert ret == (DeploymentPlan(create=[], patch=[], delete=[]), False)

        # Verify that there were no side effects even though the
        # callback function made changes to the manifests.
        assert loc_man == loc_man_bak
        assert srv_man == srv_man_bak

        # Repeat the test, but this time force an error in the callback function.
        def cb2(square_config: Config, local_manifest: dict, server_manifest: dict):
            raise ValueError()

        config.patch_callback = cb2
        _, err = await sq.compile_plan(config, k8sconfig, loc_man, srv_man)
        assert err


class TestMainOptions:
    @mock.patch.object(k8s, "post")
    @mock.patch.object(k8s, "patch")
    @mock.patch.object(k8s, "delete")
    async def test_apply_plan(self, m_delete, m_apply, m_post, config, kube_creds):
        """Simulate a successful resource update (add, patch, delete).

        To this end, create a valid (mocked) deployment plan, mock out all
        calls, and verify that all the necessary calls are made.

        The second part of the test simulates errors. This is not a separate
        test because it shares virtually all the boiler plate code.

        """
        k8sconfig: K8sConfig = kube_creds

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
            create=[DeltaCreate(meta, "create_url", {"create": "man"})],
            patch=[DeltaPatch(meta, "diff", patch)],
            delete=[DeltaDelete(meta, "delete_url", {"delete": "man"})],
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
        assert not k8sconfig.client.is_closed
        assert await sq.apply_plan(config, plan) is False
        assert k8sconfig.client.is_closed

        m_post.assert_called_once_with(k8sconfig, "create_url", {"create": "man"})
        m_apply.assert_called_once_with(k8sconfig, patch.url, patch.ops)
        m_delete.assert_called_once_with(k8sconfig, "delete_url", {"delete": "man"})

        # -----------------------------------------------------------------
        #                   Simulate An Empty Plan
        # -----------------------------------------------------------------
        # Repeat the test and ensure the function does not even ask for
        # confirmation if the plan is empty.
        reset_mocks()
        empty_plan = DeploymentPlan(create=[], patch=[], delete=[])

        # Call test function and verify that it did not try to apply
        # the empty plan.
        assert await sq.apply_plan(config, empty_plan) is False
        assert not m_post.called
        assert not m_apply.called
        assert not m_delete.called

        # -----------------------------------------------------------------
        #                   Simulate Error Scenarios
        # -----------------------------------------------------------------
        reset_mocks()

        # Make `delete` fail.
        m_delete.return_value = (None, True)
        assert await sq.apply_plan(config, plan) is True

        # Make `patch` fail.
        m_apply.return_value = (None, True)
        assert await sq.apply_plan(config, plan) is True

        # Make `post` fail.
        m_post.return_value = (None, True)
        assert await sq.apply_plan(config, plan) is True

        # Make `sort_plan` fail.
        with mock.patch.object(sq, "sort_plan") as m_sort:
            m_sort.return_value = [], True
            assert await sq.apply_plan(config, plan) is True

    def test_pick_manifests_for_plan_different_resources(self):
        """Use an orthogonal set of manifests for server and client.

        The function must pick out the correct manifests based on the
        KIND and NAMESPACE selectors. We will not use any labels here.

        """
        # Define two Pods and a Service.
        meta_ns1_pod1 = MetaManifest('v1', 'Pod', "ns1", "pod-1")
        meta_ns2_pod2 = MetaManifest('v1', 'Pod', "ns2", "pod-2")
        meta_ns1_svc1 = MetaManifest('v1', 'Service', "ns1", "svc-1")
        man_ns1_pod1 = make_manifest("Pod", "ns1", "pod-1")
        man_ns2_pod2 = make_manifest("Pod", "ns2", "pod-2")
        man_ns1_svc1 = make_manifest("Service", "ns1", "svc-1")

        # Set of manifests we will have on either the server or locally.
        square_manifests = {
            meta_ns1_pod1: man_ns1_pod1,
            meta_ns2_pod2: man_ns2_pod2,
            meta_ns1_svc1: man_ns1_svc1,
        }

        # The `idx` is purely so swap local and server manifests. This is
        # because the current test only supplies either server manifests or
        # local manifests to ensure that the set of manifests is orthogonal.
        for idx in (0, 1):
            loc, srv = (square_manifests, {}) if idx == 0 else ({}, square_manifests)

            # Select all Pods and Services in all namespaces.
            selectors = Selectors(kinds={"Pod", "Service"}, namespaces=[])
            ret = sq.pick_manifests_for_plan(loc, srv, selectors)
            assert ret[idx] == {
                meta_ns1_pod1: man_ns1_pod1,
                meta_ns2_pod2: man_ns2_pod2,
                meta_ns1_svc1: man_ns1_svc1,
            }

            # Select all Pods and Services in namespace "ns1".
            selectors = Selectors(kinds={"Pod", "Service"}, namespaces=["ns1"])
            ret = sq.pick_manifests_for_plan(loc, srv, selectors)
            assert ret[idx] == {
                meta_ns1_pod1: man_ns1_pod1,
                meta_ns1_svc1: man_ns1_svc1,
            }

            # Select all Pods and Services in namespace "ns2".
            selectors = Selectors(kinds={"Pod", "Service"}, namespaces=["ns2"])
            ret = sq.pick_manifests_for_plan(loc, srv, selectors)
            assert ret[idx] == {
                meta_ns2_pod2: man_ns2_pod2,
            }

            # Select all Pods in all namespaces.
            selectors = Selectors(kinds={"Pod"}, namespaces=[])
            ret = sq.pick_manifests_for_plan(loc, srv, selectors)
            assert ret[idx] == {
                meta_ns1_pod1: man_ns1_pod1,
                meta_ns2_pod2: man_ns2_pod2,
            }

            # Select all Pods in namespace "ns1".
            selectors = Selectors(kinds={"Pod"}, namespaces=["ns1"])
            ret = sq.pick_manifests_for_plan(loc, srv, selectors)
            assert ret[idx] == {
                meta_ns1_pod1: man_ns1_pod1,
            }

            # Select all Pods in namespace "ns2".
            selectors = Selectors(kinds={"Pod"}, namespaces=["ns2"])
            ret = sq.pick_manifests_for_plan(loc, srv, selectors)
            assert ret[idx] == {
                meta_ns2_pod2: man_ns2_pod2,
            }

            # Select all Services in namespace "ns2".
            selectors = Selectors(kinds={"Service"}, namespaces=["ns2"])
            ret = sq.pick_manifests_for_plan(loc, srv, selectors)
            assert ret[idx] == {}

    def test_pick_manifests_for_plan_same_resource_different_labels(self):
        """Must retain only the manifests that match the selectors."""
        # Define the same Pod resource but with different labels.
        meta_pod = MetaManifest('v1', 'Pod', "ns1", "pod-1")
        man_loc = make_manifest("Pod", "ns1", "pod-1", labels={"app": "local"})
        man_srv = make_manifest("Pod", "ns1", "pod-1", labels={"app": "server"})

        # The same Pod exists locally and on the server but with different labels.
        loc: SquareManifests = {meta_pod: man_loc}
        srv: SquareManifests = {meta_pod: man_srv}

        # Selector matches neither manifest: must return nothing.
        selectors = Selectors(kinds={"Pod"}, labels=["app=does-not-match"])
        s_loc, s_srv = sq.pick_manifests_for_plan(loc, srv, selectors)
        assert s_loc == s_srv == {}

        # Label selectors match either local, or server or both manifests. In
        # all cases, Square must have included in its list of manifests for
        # which to compute a plan.
        for labels in ([], ["app=local"], ["app=server"]):
            selectors = Selectors(kinds={"Pod"}, labels=labels)
            s_loc, s_srv = sq.pick_manifests_for_plan(loc, srv, selectors)
            assert s_loc == loc and s_srv == srv

    @mock.patch.object(manio, "load_manifests")
    @mock.patch.object(manio, "download")
    @mock.patch.object(sq, "pick_manifests_for_plan")
    @mock.patch.object(manio, "align_serviceaccount")
    @mock.patch.object(sq, "compile_plan")
    async def test_make_plan_full_mock(self, m_plan, m_align, m_pick, m_down, m_load,
                                       config, kube_creds):
        """Verify that `make_plan` calls the right functions with the right arguments."""
        k8sconfig: K8sConfig = kube_creds
        err_resp = (DeploymentPlan(tuple(), tuple(), tuple()), True)

        # Valid deployment plan.
        plan = DeploymentPlan(create=[], patch=[], delete=[])

        # Dummy manifests.
        loc_man = make_manifest("Pod", "default", "loc")
        srv_man = make_manifest("Pod", "default", "srv")

        # All auxiliary functions will succeed.
        local = {manio.make_meta(loc_man): loc_man}
        server = {manio.make_meta(srv_man): srv_man}
        m_load.return_value = (local, None, False)
        m_down.return_value = (server, False)
        m_pick.return_value = (local, server)
        m_plan.return_value = (plan, False)
        m_align.side_effect = lambda loc_man, _: (loc_man, False)

        # A successful DIFF only computes and prints the plan.
        assert not k8sconfig.client.is_closed
        plan, err = await sq.make_plan(config)
        assert not err
        assert k8sconfig.client.is_closed

        assert isinstance(plan, DeploymentPlan)
        m_load.assert_called_once_with(config.folder, config.selectors)
        m_down.assert_called_once_with(config, k8sconfig)
        assert m_pick.called and m_pick.call_count == 1
        m_pick.assert_called_once_with(local, server, config.selectors)
        m_plan.assert_called_once_with(config, k8sconfig, local, server)

        # Make `compile_plan` fail.
        m_plan.return_value = (None, True)
        assert await sq.make_plan(config) == err_resp

        # Make `download_manifests` fail.
        m_down.return_value = (None, True)
        assert await sq.make_plan(config) == err_resp

        # Make `load` fail.
        m_load.return_value = (None, None, True)
        assert await sq.make_plan(config) == err_resp

    @mock.patch.object(manio, "load_manifests")
    @mock.patch.object(manio, "download")
    async def test_make_plan_no_labels(self, m_down, m_load, config, kube_creds):
        """Mock the available local/server manifests and verify the plan.

        This test does not use any label selectors because there are dedicated
        tests to cover various edge cases.

        """
        # Select all Pods in all namespaces. Ignore labels.
        config.selectors = Selectors(kinds={"Pod"}, namespaces=[], labels=[])

        # Define a single resource.
        meta_pod1 = MetaManifest('v1', 'Pod', "ns1", "pod-1")
        man_pod1 = make_manifest("Pod", "ns1", "pod-1")
        meta_pod2 = MetaManifest('v1', 'Pod', "ns1", "pod-2")
        man_pod2 = make_manifest("Pod", "ns1", "pod-2")

        # Local and server manifests are in sync.
        loc: SquareManifests = {meta_pod1: man_pod1}
        srv: SquareManifests = {meta_pod1: man_pod1}
        m_load.return_value = (loc, {}, False)
        m_down.return_value = (srv, False)

        plan, err = await sq.make_plan(config)
        assert not err
        assert plan.create == [] and plan.patch == [] and plan.delete == []

        # Pod 1 exists only locally whereas Pod 2 exists only on the cluster.
        # The plan must therefore suggest to create Pod 1 and delete Pod 2.
        loc: SquareManifests = {meta_pod1: man_pod1}
        srv: SquareManifests = {meta_pod2: man_pod2}
        m_load.return_value = (loc, {}, False)
        m_down.return_value = (srv, False)

        plan, err = await sq.make_plan(config)
        assert not err
        assert plan.patch == []
        assert len(plan.create) == 1
        assert len(plan.create) == len(plan.delete) == 1

        assert plan.create[0].meta == meta_pod1
        assert plan.delete[0].meta == meta_pod2

        # Pod 1 exists locally and on the server, but their content differs.
        # Square must propose a single PATCH.
        loc: SquareManifests = {meta_pod1: man_pod1}
        srv: SquareManifests = copy.deepcopy(loc)
        loc[meta_pod1]["spec"]["foo"] = "foo"
        srv[meta_pod1]["spec"]["bar"] = "bar"
        m_load.return_value = (loc, {}, False)
        m_down.return_value = (srv, False)

        plan, err = await sq.make_plan(config)
        assert not err
        assert len(plan.create) == len(plan.delete) == 0
        assert len(plan.patch) == 1
        assert plan.patch[0].meta == meta_pod1

    @mock.patch.object(manio, "load_manifests")
    @mock.patch.object(manio, "download")
    async def test_make_plan_different_labels(self, m_down, m_load, config, kube_creds):
        """Mock the available local/server manifests and verify the plan.

        The server and local manifests both declare the same Pod resource but
        with different labels.

        """
        # Select all Pods in all namespaces. We will set labels below.
        config.selectors = Selectors(kinds={"Pod"}, namespaces=[])

        # Define the same resource twice but with different labels.
        meta_pod = MetaManifest('v1', 'Pod', "ns1", "pod-1")
        man_loc = make_manifest("Pod", "ns1", "pod-1", labels={"app": "local"})
        man_srv = make_manifest("Pod", "ns1", "pod-1", labels={"app": "server"})

        # The same Pod resource exists both locally and on the server but with
        # different labels.
        loc: SquareManifests = {meta_pod: man_loc}
        srv: SquareManifests = {meta_pod: man_srv}
        m_load.return_value = (loc, {}, False)
        m_down.return_value = (srv, False)

        # Label selector matches neither local nor server: Square must do nothing.
        config.selectors.labels = ["app=unknown"]
        plan, err = await sq.make_plan(config)
        assert not err
        assert plan.create == plan.delete == plan.patch == []

        # Label selector matches local and server: Square must PATCH.
        for labels in ([], ["app=local"], ["app=server"]):
            config.selectors.labels = labels
            plan, err = await sq.make_plan(config)
            assert not err
            assert plan.create == [] and plan.delete == [] and len(plan.patch) == 1

    @mock.patch.object(manio, "load_manifests")
    @mock.patch.object(manio, "download")
    async def test_make_plan_kind_name(self, m_down, m_load, config, kube_creds):
        """Mock the available local/server manifests and verify the plan.

        The server and local manifests both declare the same Pod resource but
        with different labels.

        """
        # Define the same resource twice but with different labels.
        meta_pod1 = MetaManifest('v1', 'Pod', "ns1", "pod-1")
        meta_pod2 = MetaManifest('v1', 'Pod', "ns1", "pod-2")
        man_pod1 = make_manifest("Pod", "ns1", "pod-1")
        man_pod2 = make_manifest("Pod", "ns1", "pod-2")

        # The same Pod resource exists both locally and on the server but with
        # different labels.
        loc: SquareManifests = {meta_pod1: man_pod1, meta_pod2: man_pod2}
        srv: SquareManifests = {}
        m_load.return_value = (loc, {}, False)
        m_down.return_value = (srv, False)

        # Selector matches both pods: Square must plan to create both.
        config.selectors = Selectors(kinds={"Pod"})
        plan, err = await sq.make_plan(config)
        assert not err
        assert plan.patch == plan.delete == []
        assert len(plan.create) == 2

        # Selector matches only one pods: Square must plan to create it.
        config.selectors = Selectors(kinds={"Pod/pod-1"})
        plan, err = await sq.make_plan(config)
        assert not err
        assert plan.patch == plan.delete == []
        assert len(plan.create) == 1
        assert plan.create[0].meta.name == "pod-1"

        # Selector matches no pods: Square must plan nothing.
        config.selectors = Selectors(kinds={"Pod/pod-1"})
        plan, err = await sq.make_plan(config)
        assert not err
        assert plan.patch == plan.patch == plan.delete == []

    @pytest.mark.parametrize("function", ("get_resources", "make_plan"))
    @mock.patch.object(manio, "load_manifests")
    @mock.patch.object(manio, "download")
    async def test_invalid_local_manifests(self, m_down, m_load,
                                           function, kube_creds, config):
        """Simulate a local Pod manifest that lacks a namespace.

        This must fail the validation in both `get_resources` and `make_plan`.
        The two functions are similar enough to be covered by a single test.

        """

        # Local Pod manifest does not specify a NAMESPACE.
        loc_man = make_manifest("Pod", None, "loc")
        loc_sqm = {manio.make_meta(loc_man): loc_man}
        m_load.return_value = (loc_sqm, "local_path", False)

        # Function must gracefully abort due to a validation failure and not
        # try to download any manifests.
        assert function in ("get_resources", "make_plan")
        if function == "get_resources":
            assert await sq.get_resources(config) is True
        else:
            plan = DeploymentPlan(tuple(), tuple(), tuple())
            assert await sq.make_plan(config) == (plan, True)

        assert not m_down.called

    @mock.patch.object(manio, "load_manifests")
    @mock.patch.object(manio, "download")
    @mock.patch.object(sq, "match_api_version")
    @mock.patch.object(manio, "strip_manifests")
    @mock.patch.object(manio, "sync")
    @mock.patch.object(manio, "save")
    async def test_get_resources_full_mock(self, m_save, m_sync, m_strip, m_mapi,
                                           m_down, m_load, kube_creds, config):
        """Basic test.

        The `get_resource` function is more of a linear script than anything
        else. We merely need to verify it calls the correct functions with the
        correct arguments and aborts at the first error.

        """
        k8sconfig: K8sConfig = kube_creds

        # Dummy manifests.
        loc_man = make_manifest("Pod", "default", "loc")
        srv_man = make_manifest("Pod", "default", "srv")

        # All auxiliary functions will succeed.
        loc_sqm = {manio.make_meta(loc_man): loc_man}
        srv_sqm = {manio.make_meta(srv_man): srv_man}

        # Simulate successful responses from the two auxiliary functions.
        # The `load` function must return empty dicts to ensure the error
        # conditions are properly coded.
        m_load.return_value = (loc_sqm, "local_path", False)
        m_down.return_value = (srv_sqm, False)
        m_mapi.return_value = ("matched", False)
        m_sync.return_value = ({"path": [("meta", "manifest")]}, False)
        m_strip.return_value = ({}, {"meta": "manifest-strip"}, False)
        m_save.return_value = False

        # `manio.load` must have been called with a wildcard selector to ensure
        # it loads _all_ resources from the local files, even if we want to
        # sync only a subset.
        load_selectors = Selectors(kinds=k8sconfig.kinds, labels=[], namespaces=[])

        # Call test function and verify it passed the correct arguments.
        assert not k8sconfig.client.is_closed
        assert await sq.get_resources(config) is False
        assert k8sconfig.client.is_closed

        m_load.assert_called_once_with(config.folder, load_selectors)
        m_down.assert_called_once_with(config, k8sconfig)
        m_mapi.assert_called_once_with(k8sconfig, loc_sqm, srv_sqm)
        m_sync.assert_called_once_with("local_path", "matched",
                                       config.selectors, config.groupby)
        m_strip.assert_called_once_with(
            config, {}, {"meta": "manifest"})
        m_save.assert_called_once_with(
            config.folder, {"path": [("meta", "manifest-strip")]}, config.priorities)

        # Simulate an error with `manio.save`.
        m_save.return_value = (None, True)
        assert await sq.get_resources(config) is True

        # Simulate an error with `manio.sync`.
        m_sync.return_value = (None, True)
        assert await sq.get_resources(config) is True

        # Simulate an error in `download_manifests`.
        m_down.return_value = (None, True)
        assert await sq.get_resources(config) is True

        # Simulate an error in `load`.
        m_load.return_value = (None, None, True)
        assert await sq.get_resources(config) is True

    @mock.patch.object(manio, "download")
    async def test_get_resources_basic(self, m_down, kube_creds, config: Config):
        """Simulate an empty local folder and some downloaded manifests."""
        # Only show INFO and above or otherwise this test will produce a
        # humongous amount of logs from all the K8s calls.
        square.square.setup_logging(2)

        config.groupby = GroupBy()
        man_path = config.folder / "_other.yaml"

        svc = "Service"
        hpa = "HorizontalPodAutoscaler"

        # Dummy manifests.
        man_svc_1 = make_manifest(svc, "ns", "svc-1", labels=dict(app="app-1"))
        man_svc_2 = make_manifest(svc, "ns", "svc-2", labels=dict(app="app-2"))
        man_hpa_1 = make_manifest(hpa, "ns", "svc-2", labels=dict(app="app-1"))

        # MetaManifests for the dummy manifests.
        meta_svc_1 = manio.make_meta(man_svc_1)
        meta_svc_2 = manio.make_meta(man_svc_2)
        meta_hpa_1 = manio.make_meta(man_hpa_1)

        # Pretend that this is what we downloaded from K8s.
        square_manifests: SquareManifests = {
            meta_svc_1: man_svc_1,
            meta_svc_2: man_svc_2,
            meta_hpa_1: man_hpa_1,
        }
        m_down.return_value = (square_manifests, False)

        # --------------------------------------------------------------------------------
        # Download all three manifest and verify that callback was called three times.
        # --------------------------------------------------------------------------------
        call_count_cb1 = 0

        def cb1(square_config: Config, manifest: dict) -> dict:
            nonlocal call_count_cb1
            call_count_cb1 += 1
            manifest["metadata"]["labels"]["cb1"] = "called"
            return manifest

        # Specify all kinds and install callback to strip manifests.
        config.selectors = Selectors(kinds={svc, hpa})
        config.strip_callback = cb1

        # Call function and verify the callback got called three times and that
        # all downloaded manifests have the new label.
        assert await sq.get_resources(config) is False
        assert call_count_cb1 == 3
        manifests = list(yaml.safe_load_all(man_path.read_text()))
        for manifest in manifests:
            assert manifest["metadata"]["labels"]["cb1"] == "called"
        del call_count_cb1

        # --------------------------------------------------------------------------------
        # Delete downloaded resources and select only HPAs this time.
        # The callback must be called once only.
        # --------------------------------------------------------------------------------
        # Delete downloaded manifests.
        man_path.unlink()

        call_count_cb2 = 0

        def cb2(square_config: Config, manifest: dict) -> dict:
            nonlocal call_count_cb2
            call_count_cb2 += 1
            manifest["metadata"]["labels"]["cb2"] = "called"
            return manifest

        # Select only HPAs and install callback to strip manifests.
        config.selectors = Selectors(kinds={hpa})
        config.strip_callback = cb2

        # Call function and verify the callback got called three times and that
        # all downloaded manifests have the new label.
        assert await sq.get_resources(config) is False
        manifests = list(yaml.safe_load_all(man_path.read_text()))
        assert call_count_cb2 == len(manifests) == 1
        for manifest in manifests:
            assert manifest["metadata"]["labels"]["cb2"] == "called"
        del call_count_cb2

        # --------------------------------------------------------------------------------
        # Keep the previously downloaded HPA resources and select only SERVICEs.
        #
        # The callback must be called three times because Square will apply it
        # to the union of all downloaded manifests that fit the selectors as
        # well as the existing local manifests.
        # --------------------------------------------------------------------------------
        assert man_path.exists()

        call_count_cb3 = 0

        def cb3(square_config: Config, manifest: dict) -> dict:
            nonlocal call_count_cb3
            call_count_cb3 += 1
            manifest["metadata"]["labels"]["cb3"] = "called"
            return manifest

        # Select only HPAs and install callback to strip manifests.
        config.selectors = Selectors(kinds={svc})
        config.strip_callback = cb3

        # Call function and verify the callback got called three times and that
        # all downloaded manifests have the new label.
        assert await sq.get_resources(config) is False
        manifests = list(yaml.safe_load_all(man_path.read_text()))
        assert call_count_cb3 == len(manifests) == 3
        for manifest in manifests:
            assert manifest["metadata"]["labels"]["cb3"] == "called"
