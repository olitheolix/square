import copy
import os
import types
import unittest.mock as mock

import pytest
import square.k8s as k8s
import square.manio as manio
import square.square as square
from square.dtypes import (
    SUPPORTED_KINDS, DeltaCreate, DeltaDelete, DeltaPatch, DeploymentPlan,
    JsonPatch, MetaManifest, Selectors,
)
from square.k8s import urlpath

from .test_helpers import make_manifest


class TestLogging:
    def test_setup_logging(self):
        """Basic tests - mostly ensure that function runs."""

        # Test function must accept all log levels.
        for level in range(10):
            square.setup_logging(level)


class TestBasic:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    @classmethod
    def teardown_class(cls):
        pass

    def setup_method(self, method):
        # All tests must run relative to this folder because the script makes
        # assumptions about the location of the templates, tf, etc folder.
        os.chdir(os.path.dirname(os.path.abspath(__file__)))

    def test_find_namespace_orphans(self):
        """Return all resource manifests that belong to non-existing
        namespaces.

        This function will be useful to sanity check the local deployments
        manifest to avoid cases where users define resources in a namespace but
        forget to define that namespace (or mis-spell it).

        """
        fun = square.find_namespace_orphans

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

    def test_print_deltas(self):
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
        assert square.print_deltas(plan) == (None, False)


class TestPartition:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

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
        assert square.partition_manifests(local_man, cluster_man) == (plan, False)

    def test_partition_manifests_add_delete(self):
        """Local and server manifests are orthogonal sets.

        This must produce a plan where all local resources will be created, all
        cluster resources deleted and none patched.

        """
        fun = square.partition_manifests

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
        fun = square.partition_manifests

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
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def test_make_patch_empty(self):
        """Basic test: compute patch between two identical resources."""
        # Setup.
        kind, ns, name = 'Deployment', 'ns', 'foo'
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # PATCH URLs require the resource name at the end of the request path.
        url = urlpath(config, kind, ns)[0] + f'/{name}'

        # The patch must be empty for identical manifests.
        loc = srv = make_manifest(kind, ns, name)
        data, err = square.make_patch(config, loc, srv)
        assert (data, err) == (JsonPatch(url, []), False)
        assert isinstance(data, JsonPatch)

    def test_make_patch_incompatible(self):
        """Must not try to compute diffs for incompatible manifests.

        For instance, refuse to compute a patch when one manifest has kind
        "Namespace" and the other "Deployment". The same is true for
        "apiVersion", "metadata.name" and "metadata.namespace".

        """
        # Setup.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # Demo manifest.
        srv = make_manifest('Deployment', 'Namespace', 'name')

        # `apiVersion` must match.
        loc = copy.deepcopy(srv)
        loc['apiVersion'] = 'mismatch'
        assert square.make_patch(config, loc, srv) == (None, True)

        # `kind` must match.
        loc = copy.deepcopy(srv)
        loc['kind'] = 'Mismatch'
        assert square.make_patch(config, loc, srv) == (None, True)

        # `name` must match.
        loc = copy.deepcopy(srv)
        loc['metadata']['name'] = 'mismatch'
        assert square.make_patch(config, loc, srv) == (None, True)

        # `namespace` must match.
        loc = copy.deepcopy(srv)
        loc['metadata']['namespace'] = 'mismatch'
        assert square.make_patch(config, loc, srv) == (None, True)

    def test_make_patch_special(self):
        """Namespace, ClusterRole(Bindings) etc are special.

        What makes them special is that they exist outside namespaces.
        Therefore, they will/must not contain a `metadata.Namespace` attribute
        and require special treatment in `make_patch`.

        """
        # Generic fixtures; values are irrelevant.
        config = types.SimpleNamespace(url='http://examples.com/', version="1.10")
        name = "foo"

        for kind in ["Namespace", "ClusterRole"]:
            # Determine the resource path so we can verify it later.
            url = urlpath(config, kind, None)[0] + f'/{name}'

            # The patch between two identical manifests must be empty but valid.
            loc = srv = make_manifest(kind, None, name)
            assert square.make_patch(config, loc, srv) == ((url, []), False)

            # Create two almost identical manifests, except the second one has
            # the forbidden `metadata.namespace` attribute. This must fail.
            loc = make_manifest(kind, None, name)
            srv = copy.deepcopy(loc)
            loc['metadata']['namespace'] = 'foo'
            data, err = square.make_patch(config, loc, srv)
            assert data is None and err is not None

            # Create two almost identical manifests, except the second one has
            # different `metadata.labels`. This must succeed.
            loc = make_manifest(kind, None, name)
            srv = copy.deepcopy(loc)
            loc['metadata']['labels'] = {"key": "value"}

            data, err = square.make_patch(config, loc, srv)
            assert err is False and len(data) > 0

    @mock.patch.object(k8s, "urlpath")
    def test_make_patch_error_urlpath(self, m_url):
        """Coverage gap: simulate `urlpath` error."""
        # Setup.
        kind, ns, name = "Deployment", "ns", "foo"
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # Simulate `urlpath` error.
        m_url.return_value = (None, True)

        # Test function must return with error.
        loc = srv = make_manifest(kind, ns, name)
        assert square.make_patch(config, loc, srv) == (None, True)


class TestPlan:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def test_make_patch_ok(self):
        """Compute patch between two manifests.

        This test function first verifies that the patch between two identical
        manifests is empty. The second used two manifests that have different
        labels. This must produce two patch operations, one to remove the old
        label and one to add the new ones.

        """
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # Two valid manifests.
        kind, namespace, name = "Deployment", "namespace", "name"
        srv = make_manifest(kind, namespace, name)
        loc = make_manifest(kind, namespace, name)
        srv["metadata"]["labels"] = {"old": "old"}
        loc["metadata"]["labels"] = {"new": "new"}

        # The Patch between two identical manifests must be a No-Op.
        expected = JsonPatch(
            url=urlpath(config, kind, namespace)[0] + f"/{name}",
            ops=[],
        )
        assert square.make_patch(config, loc, loc) == (expected, False)

        # The patch between `srv` and `loc` must remove the old label and add
        # the new one.
        expected = JsonPatch(
            url=urlpath(config, kind, namespace)[0] + f"/{name}",
            ops=[
                {'op': 'remove', 'path': '/metadata/labels/old'},
                {'op': 'add', 'path': '/metadata/labels/new', 'value': 'new'}
            ]
        )
        assert square.make_patch(config, loc, srv) == (expected, False)

    def test_make_patch_err(self):
        """Verify error cases with invalid or incompatible manifests."""
        valid_cfg = k8s.Config("url", "token", "cert", "client_cert", "1.10", "")
        invalid_cfg = k8s.Config("url", "token", "cert", "client_cert", "invalid", "")

        # Create two valid manifests, then stunt one in such a way that
        # `manio.strip` will reject it.
        kind, namespace, name = "Deployment", "namespace", "name"
        valid = make_manifest(kind, namespace, name)
        invalid = make_manifest(kind, namespace, name)
        del invalid["kind"]

        # Must handle errors from `manio.strip`.
        assert square.make_patch(valid_cfg, valid, invalid) == (None, True)
        assert square.make_patch(valid_cfg, invalid, valid) == (None, True)
        assert square.make_patch(valid_cfg, invalid, invalid) == (None, True)

        # Must handle `urlpath` errors.
        assert square.make_patch(invalid_cfg, valid, valid) == (None, True)

        # Must handle incompatible manifests, ie manifests that do not belong
        # to the same resource.
        valid_a = make_manifest(kind, namespace, "bar")
        valid_b = make_manifest(kind, namespace, "foo")
        assert square.make_patch(valid_cfg, valid_a, valid_b) == (None, True)

    def test_compile_plan_create_delete_ok(self):
        """Test a plan that creates and deletes resource, but not patches any.

        To do this, the local and server resources are all distinct. As a
        result, the returned plan must dictate that all local resources shall
        be created, all server resources deleted, and none patched.

        """
        # Create vanilla `Config` instance.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # Allocate arrays for the MetaManifests and resource URLs.
        meta = [None] * 5
        url = [None] * 5

        # Define Namespace "ns1" with 1 deployment.
        meta[0] = MetaManifest('v1', 'Namespace', None, 'ns1')
        meta[1] = MetaManifest('v1', 'Deployment', 'ns1', 'res_0')

        # Define Namespace "ns2" with 2 deployments.
        meta[2] = MetaManifest('v1', 'Namespace', None, 'ns2')
        meta[3] = MetaManifest('v1', 'Deployment', 'ns2', 'res_1')
        meta[4] = MetaManifest('v1', 'Deployment', 'ns2', 'res_2')

        # Determine the K8s resource urls for those that will be added.
        upb = urlpath
        url[0] = upb(config, meta[0].kind, meta[0].namespace)[0]
        url[1] = upb(config, meta[1].kind, meta[1].namespace)[0]

        # Determine the K8s resource URLs for those that will be deleted. They
        # are slightly different because DELETE requests expect a URL path that
        # ends with the resource, eg
        # "/api/v1/namespaces/ns2"
        # instead of
        # "/api/v1/namespaces".
        url[2] = upb(config, meta[2].kind, meta[2].namespace)[0] + "/" + meta[2].name
        url[3] = upb(config, meta[3].kind, meta[3].namespace)[0] + "/" + meta[3].name
        url[4] = upb(config, meta[4].kind, meta[4].namespace)[0] + "/" + meta[4].name

        # Compile local and server manifests that have no resource overlap.
        # This will ensure that we have to create all the local resources,
        # delete all the server resources and path nothing.
        loc_man = {meta[0]: "0", meta[1]: "1"}
        srv_man = {meta[2]: "2", meta[3]: "3", meta[4]: "4"}

        # The resources require a manifest to specify the terms of deletion.
        # This is currently hard coded into the function.
        del_opts = {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "gracePeriodSeconds": 0,
            "orphanDependents": False,
        }

        # Resources from local files must be created, resources on server must
        # be deleted.
        expected = DeploymentPlan(
            create=[
                DeltaCreate(meta[0], url[0], loc_man[meta[0]]),
                DeltaCreate(meta[1], url[1], loc_man[meta[1]]),
            ],
            patch=[],
            delete=[
                DeltaDelete(meta[2], url[2], del_opts),
                DeltaDelete(meta[3], url[3], del_opts),
                DeltaDelete(meta[4], url[4], del_opts),
            ],
        )
        assert square.compile_plan(config, loc_man, srv_man) == (expected, False)

    @mock.patch.object(square, "partition_manifests")
    def test_compile_plan_create_delete_err(self, m_part):
        """Simulate `urlpath` errors"""
        # Invalid configuration. We will use it to trigger an error in `urlpath`.
        cfg_invalid = k8s.Config("url", "token", "cert", "cert", "invalid", "")

        # Valid ManifestMeta and dummy manifest dict.
        meta = manio.make_meta(make_manifest("Deployment", "ns", "name"))
        man = {meta: None}

        # Pretend we only have to "create" resources, and then trigger the
        # `urlpath` error in its code path.
        m_part.return_value = (
            DeploymentPlan(create=[meta], patch=[], delete=[]),
            False,
        )
        assert square.compile_plan(cfg_invalid, man, man) == (None, True)

        # Pretend we only have to "delete" resources, and then trigger the
        # `urlpath` error in its code path.
        m_part.return_value = (
            DeploymentPlan(create=[], patch=[], delete=[meta]),
            False,
        )
        assert square.compile_plan(cfg_invalid, man, man) == (None, True)

    def test_compile_plan_patch_no_diff(self):
        """Test a plan that patches all resources.

        To do this, the local and server resources are identical. As a
        result, the returned plan must nominate all manifests for patching, and
        none to create and delete.

        """
        # Create vanilla `Config` instance.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # Allocate arrays for the MetaManifests.
        meta = [None] * 4

        # Define two Namespace with 1 deployment each.
        meta[0] = MetaManifest('v1', 'Namespace', None, 'ns1')
        meta[1] = MetaManifest('v1', 'Deployment', 'ns1', 'res_0')
        meta[2] = MetaManifest('v1', 'Namespace', None, 'ns2')
        meta[3] = MetaManifest('v1', 'Deployment', 'ns2', 'res_1')

        # Determine the K8s resource URLs for patching. Those URLs must contain
        # the resource name as the last path element, eg "/api/v1/namespaces/ns1"
        url = [
            urlpath(config, _.kind, _.namespace)[0] + f"/{_.name}"
            for _ in meta
        ]

        # Local and server manifests are identical. The plan must therefore
        # only nominate patches but nothing to create or delete.
        loc_man = srv_man = {
            meta[0]: make_manifest("Namespace", None, "ns1"),
            meta[1]: make_manifest("Deployment", "ns1", "res_0"),
            meta[2]: make_manifest("Namespace", None, "ns2"),
            meta[3]: make_manifest("Deployment", "ns2", "res_1"),
        }
        expected = DeploymentPlan(
            create=[],
            patch=[
                DeltaPatch(meta[0], "", JsonPatch(url[0], [])),
                DeltaPatch(meta[1], "", JsonPatch(url[1], [])),
                DeltaPatch(meta[2], "", JsonPatch(url[2], [])),
                DeltaPatch(meta[3], "", JsonPatch(url[3], [])),
            ],
            delete=[]
        )
        assert square.compile_plan(config, loc_man, srv_man) == (expected, False)

    def test_compile_plan_patch_with_diff(self):
        """Test a plan that patches all resources.

        To do this, the local and server resources are identical. As a
        result, the returned plan must nominate all manifests for patching, and
        none to create and delete.

        """
        # Create vanilla `Config` instance.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # Define a single resource.
        meta = MetaManifest('v1', 'Namespace', None, 'ns1')

        # Local and server manifests have the same resources but their
        # definition differs. This will ensure a non-empty patch in the plan.
        loc_man = {meta: make_manifest("Namespace", None, "ns1")}
        srv_man = {meta: make_manifest("Namespace", None, "ns1")}
        loc_man[meta]["metadata"]["labels"] = {"foo": "foo"}
        srv_man[meta]["metadata"]["labels"] = {"bar": "bar"}

        # Compute the JSON patch and textual diff to populated the expected
        # output structure below.
        patch, err = square.make_patch(config, loc_man[meta], srv_man[meta])
        assert not err
        diff_str, err = manio.diff(config, loc_man[meta], srv_man[meta])
        assert not err

        # Verify the test function returns the correct Patch and diff.
        expected = DeploymentPlan(
            create=[],
            patch=[DeltaPatch(meta, diff_str, patch)],
            delete=[]
        )
        assert square.compile_plan(config, loc_man, srv_man) == (expected, False)

    @mock.patch.object(square, "partition_manifests")
    @mock.patch.object(manio, "diff")
    @mock.patch.object(square, "make_patch")
    def test_compile_plan_err(self, m_apply, m_plan, m_part):
        """Use mocks for the internal function calls to simulate errors."""
        # Create vanilla `Config` instance.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # Define a single resource and valid dummy return value for
        # `square.partition_manifests`.
        meta = MetaManifest('v1', 'Namespace', None, 'ns1')
        plan = DeploymentPlan(create=[], patch=[meta], delete=[])

        # Local and server manifests have the same resources but their
        # definition differs. This will ensure a non-empty patch in the plan.
        loc_man = srv_man = {meta: make_manifest("Namespace", None, "ns1")}

        # Simulate an error in `compile_plan`.
        m_part.return_value = (None, True)
        assert square.compile_plan(config, loc_man, srv_man) == (None, True)

        # Simulate an error in `diff`.
        m_part.return_value = (plan, False)
        m_plan.return_value = (None, True)
        assert square.compile_plan(config, loc_man, srv_man) == (None, True)

        # Simulate an error in `make_patch`.
        m_part.return_value = (plan, False)
        m_plan.return_value = ("some string", False)
        m_apply.return_value = (None, True)
        assert square.compile_plan(config, loc_man, srv_man) == (None, True)


class TestMainOptions:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    @mock.patch.object(manio, "load")
    @mock.patch.object(manio, "download")
    @mock.patch.object(square, "compile_plan")
    @mock.patch.object(k8s, "post")
    @mock.patch.object(k8s, "patch")
    @mock.patch.object(k8s, "delete")
    def test_main_apply(self, m_delete, m_apply, m_post, m_plan, m_down, m_load):
        """Simulate a successful resource update (add, patch delete).

        To this end, create a valid (mocked) deployment plan, mock out all
        calls, and verify that all the necessary calls are made.

        The second part of the test simulates errors. This is not a separate
        test because it shares virtually all the boiler plate code.
        """
        # Valid client config and MetaManifest.
        cname = "clustername"
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", cname)
        meta = manio.make_meta(make_manifest("Deployment", "ns", "name"))

        # Valid Patch.
        patch = JsonPatch(
            url="patch_url",
            ops=[
                {'op': 'remove', 'path': '/metadata/labels/old'},
                {'op': 'add', 'path': '/metadata/labels/new', 'value': 'new'},
            ],
        )

        # Valid deployment plan.
        plan = DeploymentPlan(
            create=[DeltaCreate(meta, "create_url", "create_man")],
            patch=[DeltaPatch(meta, "diff", patch)],
            delete=[DeltaDelete(meta, "delete_url", "delete_man")],
        )

        def reset_mocks():
            m_down.reset_mock()
            m_load.reset_mock()
            m_plan.reset_mock()
            m_post.reset_mock()
            m_apply.reset_mock()
            m_delete.reset_mock()

            # Pretend that all K8s requests succeed.
            m_down.return_value = ("server", False)
            m_load.return_value = ("local", None, False)
            m_plan.return_value = (plan, False)
            m_post.return_value = (None, False)
            m_apply.return_value = (None, False)
            m_delete.return_value = (None, False)

        # The arguments to the test function will always be the same in this test.
        selectors = Selectors(["kinds"], ["ns"], {("foo", "bar"), ("x", "y")})
        args = config, "client", "folder", selectors

        # Square must never create/patch/delete anything if the user did not
        # answer "yes".
        reset_mocks()
        with mock.patch.object(square, 'input', lambda _: "wrong answer"):
            assert square.main_apply(*args) == (None, True)
        assert not m_load.post.called
        assert not m_load.patch.called
        assert not m_load.delete.called

        # Update the K8s resources and verify that the test functions made the
        # corresponding calls to K8s.
        reset_mocks()
        with mock.patch.object(square, 'input', lambda _: cname):
            assert square.main_apply(*args) == (None, False)
        m_load.assert_called_once_with("folder", selectors)
        m_down.assert_called_once_with(config, "client", selectors)
        m_plan.assert_called_once_with(config, "local", "server")
        m_post.assert_called_once_with("client", "create_url", "create_man")
        m_apply.assert_called_once_with("client", patch.url, patch.ops)
        m_delete.assert_called_once_with("client", "delete_url", "delete_man")

        # -----------------------------------------------------------------
        #                   Simulate Error Scenarios
        # -----------------------------------------------------------------
        # Make `delete` fail.
        m_delete.return_value = (None, True)
        with mock.patch.object(square, 'input'):
            assert square.main_apply(*args) == (None, True)

        # Make `patch` fail.
        m_apply.return_value = (None, True)
        with mock.patch.object(square, 'input'):
            assert square.main_apply(*args) == (None, True)

        # Make `post` fail.
        m_post.return_value = (None, True)
        with mock.patch.object(square, 'input'):
            assert square.main_apply(*args) == (None, True)

        # Make `compile_plan` fail.
        m_plan.return_value = (None, True)
        with mock.patch.object(square, 'input'):
            assert square.main_apply(*args) == (None, True)

        # Make `download_manifests` fail.
        m_down.return_value = (None, True)
        with mock.patch.object(square, 'input'):
            assert square.main_apply(*args) == (None, True)

        # Make `load` fail.
        m_load.return_value = (None, None, True)
        with mock.patch.object(square, 'input'):
            assert square.main_apply(*args) == (None, True)

    @mock.patch.object(manio, "load")
    @mock.patch.object(manio, "download")
    @mock.patch.object(square, "compile_plan")
    def test_main_plan(self, m_plan, m_down, m_load):
        """Basic test.

        This function does hardly anything to begin with, so we will merely
        verify it calls the correct functions with the correct arguments and
        handles errors correctly.

        """
        # Valid deployment plan.
        plan = DeploymentPlan(create=[], patch=[], delete=[])

        # All auxiliary functions will succeed.
        m_load.return_value = ("local", None, False)
        m_down.return_value = ("server", False)
        m_plan.return_value = (plan, False)

        # The arguments to the test function will always be the same in this test.
        selectors = Selectors(["kinds"], ["ns"], {("foo", "bar"), ("x", "y")})
        args = "config", "client", "folder", selectors

        # A successfull DIFF only computes and prints the plan.
        assert square.main_plan(*args) == (None, False)
        m_load.assert_called_once_with("folder", selectors)
        m_down.assert_called_once_with("config", "client", selectors)
        m_plan.assert_called_once_with("config", "local", "server")

        # Make `compile_plan` fail.
        m_plan.return_value = (None, True)
        assert square.main_plan(*args) == (None, True)

        # Make `download_manifests` fail.
        m_down.return_value = (None, True)
        assert square.main_plan(*args) == (None, True)

        # Make `load` fail.
        m_load.return_value = (None, None, True)
        assert square.main_plan(*args) == (None, True)

    @mock.patch.object(manio, "load")
    @mock.patch.object(manio, "download")
    @mock.patch.object(manio, "sync")
    @mock.patch.object(manio, "save")
    def test_main_get(self, m_save, m_sync, m_down, m_load):
        """Basic test.

        This function does hardly anything to begin with, so we will merely
        verify it calls the correct functions with the correct arguments and
        handles errors.

        """
        # Simulate successful responses from the two auxiliary functions.
        # The `load` function must return empty dicts to ensure the error
        # conditions are properly coded.
        m_load.return_value = ("local", {}, False)
        m_down.return_value = ("server", False)
        m_sync.return_value = ("synced", False)
        m_save.return_value = (None, False)

        # The arguments to the test function will always be the same in this test.
        selectors = Selectors(["kinds"], ["ns"], {("foo", "bar"), ("x", "y")})
        args = "config", "client", "folder", selectors

        # Call test function and verify it passed the correct arguments.
        assert square.main_get(*args) == (None, False)
        m_load.assert_called_once_with("folder", selectors)
        m_down.assert_called_once_with("config", "client", selectors)
        m_sync.assert_called_once_with({}, "server", selectors)
        m_save.assert_called_once_with("folder", "synced")

        # Simulate an error with `manio.save`.
        m_save.return_value = (None, True)
        assert square.main_get(*args) == (None, True)

        # Simulate an error with `manio.sync`.
        m_sync.return_value = (None, True)
        assert square.main_get(*args) == (None, True)

        # Simulate an error in `download_manifests`.
        m_down.return_value = (None, True)
        assert square.main_get(*args) == (None, True)

        # Simulate an error in `load`.
        m_load.return_value = (None, None, True)
        assert square.main_get(*args) == (None, True)


class TestMain:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    @mock.patch.object(square, "k8s")
    @mock.patch.object(square, "main_get")
    @mock.patch.object(square, "main_plan")
    @mock.patch.object(square, "main_apply")
    def test_main_valid_options(self, m_apply, m_plan, m_get, m_k8s):
        """Simulate sane program invocation.

        This test verifies that the bootstrapping works and the correct
        `main_*` function will be called with the correct parameters.

        """
        # Dummy configuration.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # Mock all calls to the K8s API.
        m_k8s.load_auto_config.return_value = config
        m_k8s.session.return_value = "client"
        m_k8s.version.return_value = (config, False)

        # Pretend all main functions return success.
        m_get.return_value = (None, False)
        m_plan.return_value = (None, False)
        m_apply.return_value = (None, False)

        # Simulate all input options.
        for option in ["get", "plan", "apply"]:
            args = ("square.py", option, "deployment", "service", "--folder", "myfolder")
            with mock.patch("sys.argv", args):
                square.main()
            del args

        # Every main function must have been called exactly once.
        selectors = Selectors(["Deployment", "Service"], None, set())
        args = config, "client", "myfolder", selectors
        m_get.assert_called_once_with(*args)
        m_plan.assert_called_once_with(*args)
        m_apply.assert_called_once_with(*args)

    def test_main_version(self):
        """Simulate "version" command."""
        with mock.patch("sys.argv", ("square.py", "version")):
            assert square.main() == 0

    @mock.patch.object(square, "k8s")
    def test_main_invalid_option(self, m_k8s):
        """Simulate a missing or unknown option.

        Either way, the program must abort with a non-zero exit code.

        """
        # Do not pass any option.
        with mock.patch("sys.argv", ["square.py"]):
            with pytest.raises(SystemExit) as err:
                square.main()
            assert err.value.code == 2

        # Pass an invalid option.
        with mock.patch("sys.argv", ["square.py", "invalid-option"]):
            with pytest.raises(SystemExit) as err:
                square.main()
            assert err.value.code == 2

    @mock.patch.object(square, "k8s")
    @mock.patch.object(square, "main_get")
    @mock.patch.object(square, "main_plan")
    @mock.patch.object(square, "main_apply")
    def test_main_nonzero_exit_on_error(self, m_apply, m_plan, m_get, m_k8s):
        """Simulate sane program invocation.

        This test verifies that the bootstrapping works and the correct
        `main_*` function will be called with the correct parameters. However,
        each of those `main_*` functions returns with an error which means
        `square.main` must return with a non-zero exit code.

        """
        # Dummy configuration.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # Mock all calls to the K8s API.
        m_k8s.load_auto_config.return_value = config
        m_k8s.session.return_value = "client"
        m_k8s.version.return_value = (config, False)

        # Pretend all main functions return errors.
        m_get.return_value = (None, True)
        m_plan.return_value = (None, True)
        m_apply.return_value = (None, True)

        # Simulate all input options.
        for option in ["get", "plan", "apply"]:
            with mock.patch("sys.argv", ["square.py", option, "ns"]):
                assert square.main() == 1

    @mock.patch.object(square, "k8s")
    @mock.patch.object(square, "parse_commandline_args")
    def test_main_invalid_option_in_main(self, m_cmd, m_k8s):
        """Simulate an option that `square` does not know about.

        This is a somewhat pathological test and exists primarily to close a
        harmless gap in the test coverage.

        """
        # Dummy configuration.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # Mock all calls to the K8s API.
        m_k8s.load_auto_config.return_value = config
        m_k8s.session.return_value = "client"
        m_k8s.version.return_value = (config, False)

        # Pretend all main functions return errors.
        m_cmd.return_value = types.SimpleNamespace(
            verbosity=0, parser="invalid", kubeconfig="conf", ctx="ctx",
            folder=None, kinds=None, namespaces=None, labels=set()
        )
        assert square.main() == 1

    @mock.patch.object(square, "k8s")
    def test_main_version_error(self, m_k8s):
        """Program must abort if it cannot download the K8s version."""
        # Mock all calls to the K8s API.
        m_k8s.version.return_value = (None, True)

        with mock.patch("sys.argv", ["square.py", "get", "deploy"]):
            assert square.main() == 1

    def test_parse_commandline_args_basic(self):
        """Must correctly expand eg "svc" -> "Service" and remove duplicates."""
        with mock.patch("sys.argv", ["square.py", "get", "deploy", "svc"]):
            ret = square.parse_commandline_args()
            assert ret.kinds == ["Deployment", "Service"]

        # Specify Service twice (once as "svc" and once as "Service"). The
        # duplicate must be removed.
        with mock.patch("sys.argv", ["square.py", "get", "service", "deploy", "svc"]):
            ret = square.parse_commandline_args()
            assert ret.kinds == ["Service", "Deployment"]

    def test_parse_commandline_args_all(self):
        """The "all" resource must expand to all supported resource kinds."""
        # Specify "all" resources.
        with mock.patch("sys.argv", ["square.py", "get", "all"]):
            ret = square.parse_commandline_args()
            assert ret.kinds == list(SUPPORTED_KINDS)

        # Must ignore duplicates.
        with mock.patch("sys.argv", ["square.py", "get", "all", "svc", "all"]):
            ret = square.parse_commandline_args()
            assert ret.kinds == list(SUPPORTED_KINDS)

    def test_parse_commandline_args_invalid(self):
        """An invalid resource name must abort the program."""
        with mock.patch("sys.argv", ["square.py", "get", "invalid"]):
            with pytest.raises(SystemExit):
                square.parse_commandline_args()

    def test_parse_commandline_args_labels_valid(self):
        """The labels must be returned as (name, value) tuples."""
        # No labels.
        with mock.patch("sys.argv", ["square.py", "get", "all"]):
            ret = square.parse_commandline_args()
            assert ret.labels == tuple()

        # One label.
        with mock.patch("sys.argv", ["square.py", "get", "all", "-l", "foo=bar"]):
            ret = square.parse_commandline_args()
            assert ret.labels == (("foo", "bar"),)

        # Two labels.
        with mock.patch("sys.argv", ["square.py", "get", "all", "-l", "foo=bar", "x=y"]):
            ret = square.parse_commandline_args()
            assert ret.labels == (("foo", "bar"), ("x", "y"))

    def test_parse_commandline_args_labels_invalid(self):
        """Must abort on invalid labels."""
        invalid_labels = (
            "foo", "foo=", "=foo", "foo=bar=foobar", "foo==bar",
            "fo/o=bar",
        )
        for label in invalid_labels:
            with mock.patch("sys.argv", ["square.py", "get", "all", "-l", label]):
                with pytest.raises(SystemExit):
                    square.parse_commandline_args()

    def test_parse_commandline_args_kubeconfig(self):
        """Use the correct Kubeconfig file."""
        # Backup environment variables and set a custom KUBECONFIG value.
        new_env = os.environ.copy()

        # Populate the environment with a KUBECONFIG.
        new_env["KUBECONFIG"] = "envvar"
        with mock.patch.dict("os.environ", values=new_env, clear=True):
            # Square must use the supplied Kubeconfig file and ignore the
            # environment variable.
            with mock.patch("sys.argv", ["square.py", "get", "svc", "--kubeconfig", "/file"]):  # noqa
                ret = square.parse_commandline_args()
                assert ret.kubeconfig == "/file"

            # Square must fall back to the KUBECONFIG environment variable.
            with mock.patch("sys.argv", ["square.py", "get", "svc"]):
                ret = square.parse_commandline_args()
                assert ret.kubeconfig == "envvar"

        # Square must raise an error without an explicit kubeconfig
        # parameters and environment variable.
        del new_env["KUBECONFIG"]
        with mock.patch.dict("os.environ", values=new_env, clear=True):
            with mock.patch("sys.argv", ["square.py", "get", "svc"]):
                with pytest.raises(SystemExit):
                    square.parse_commandline_args()

    def test_parse_commandline_args_folder(self):
        """Use the correct manifest folder."""
        # Backup environment variables and set a custom SQUARE_FOLDER value.
        new_env = os.environ.copy()

        # Populate the environment with a SQUARE_FOLDER.
        new_env["SQUARE_FOLDER"] = "envvar"
        with mock.patch.dict("os.environ", values=new_env, clear=True):
            # Square must use the supplied value and ignore the environment variable.
            with mock.patch("sys.argv", ["square.py", "get", "svc", "--folder", "/tmp"]):
                ret = square.parse_commandline_args()
                assert ret.folder == "/tmp"

            # Square must fall back to SQUARE_FOLDER if it exists.
            with mock.patch("sys.argv", ["square.py", "get", "svc"]):
                ret = square.parse_commandline_args()
                assert ret.folder == "envvar"

        # Square must default to "./" in the absence of "--folder" and SQUARE_FOLDER.
        # parameters and environment variable.
        del new_env["SQUARE_FOLDER"]
        with mock.patch.dict("os.environ", values=new_env, clear=True):
            with mock.patch("sys.argv", ["square.py", "get", "svc"]):
                ret = square.parse_commandline_args()
                assert ret.folder == "./"

    def test_user_confirmed(self):
        # "yes" must return True.
        with mock.patch.object(square, 'input', lambda _: "yes"):
            assert square.user_confirmed("yes") is True

        # Everything other than "yes" must return False.
        answers = ("YES", "", "y", "ye", "yess", "blah")
        for answer in answers:
            with mock.patch.object(square, 'input', lambda _: answer):
                assert square.user_confirmed("yes") is False

        # Must gracefully handle keyboard interrupts and return False.
        with mock.patch.object(square, 'input') as m_input:
            m_input.side_effect = KeyboardInterrupt
            assert square.user_confirmed("yes") is False
