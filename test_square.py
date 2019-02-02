import copy
import os
import types
import unittest.mock as mock

import k8s
import manio
import square
from dtypes import MetaManifest, Patch, RetVal
from test_helpers import make_manifest


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
        assert fun(man) == RetVal(data=man, err=None)

        # Two namespaces - neither is orphaned by definition.
        man = {
            MetaManifest('v1', 'Namespace', None, 'ns1'),
            MetaManifest('v1', 'Namespace', None, 'ns2'),
        }
        assert fun(man) == RetVal(data=set(), err=None)

        # Two deployments, only one of which is inside a defined Namespace.
        man = {
            MetaManifest('v1', 'Deployment', 'ns1', 'foo'),
            MetaManifest('v1', 'Deployment', 'ns2', 'bar'),
            MetaManifest('v1', 'Namespace', None, 'ns1'),
        }
        assert fun(man) == RetVal(
            data={MetaManifest('v1', 'Deployment', 'ns2', 'bar')},
            err=None,
        )

    def test_print_deltas(self):
        """Just verify it runs.

        There is nothing really to tests here because the function only prints
        strings to the terminal. Therefore, we will merely ensure that all code
        paths run without error.

        """
        meta = manio.make_meta(make_manifest("Deployment", "ns", "name"))
        patch = square.Patch(
            url="url",
            ops=[
                {'op': 'remove', 'path': '/metadata/labels/old'},
                {'op': 'add', 'path': '/metadata/labels/new', 'value': 'new'}
            ],
        )
        plan = square.DeploymentPlan(
            create=[square.DeltaCreate(meta, "url", "manifest")],
            patch=[square.DeltaPatch(meta, "diff", patch)],
            delete=[square.DeltaDelete(meta, "url", "manifest")],
        )
        assert square.print_deltas(plan) == RetVal(None, False)


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
        plan = square.DeploymentPlan(create=[], patch=list(local_man.keys()), delete=[])
        assert square.partition_manifests(local_man, cluster_man) == RetVal(plan, False)

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
        plan = square.DeploymentPlan(
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
        assert fun(local_man, cluster_man) == RetVal(plan, False)

    def test_partition_manifests_patch_delete(self):
        """Create plan with resources to delete and patch.

        The local manifests are a strict subset of the cluster. The deployment
        plan must therefore not create any resources, delete everything absent
        from the local manifests and mark the rest for patching.

        """
        fun = square.partition_manifests

        # The local manifests are a subset of the server'. Therefore, the plan
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
        plan = square.DeploymentPlan(
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
        assert fun(local_man, cluster_man) == RetVal(plan, False)


class TestUrlPathBuilder:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def test_supported_resources_versions(self):
        """Verify the global variables.

        Those variables specify the supported K8s versions and resource types.

        """
        assert square.SUPPORTED_VERSIONS == ("1.9", "1.10")
        assert square.SUPPORTED_KINDS == ("Namespace", "Service", "Deployment")

    def test_urlpath_ok(self):
        """Must work for all supported K8s versions and resources."""
        Config = k8s.Config
        for version in square.SUPPORTED_VERSIONS:
            cfg = Config("url", "token", "ca_cert", "client_cert", version)
            for kind in square.SUPPORTED_KINDS:
                for ns in (None, "foo-namespace"):
                    path, err = square.urlpath(cfg, kind, ns)

                # Verify.
                assert err is False
                assert isinstance(path, str)

    def test_urlpath_err(self):
        """Test various error scenarios."""
        Config = k8s.Config

        # Valid version but invalid resource kind or invalid namespace spelling.
        for version in square.SUPPORTED_VERSIONS:
            cfg = Config("url", "token", "ca_cert", "client_cert", version)

            # Invalid resource kind.
            assert square.urlpath(cfg, "fooresource", "ns") == RetVal(None, True)

            # Namespace names must be all lower case (K8s imposes this)...
            assert square.urlpath(cfg, "Deployment", "namEspACe") == RetVal(None, True)

        # Invalid version.
        cfg = Config("url", "token", "ca_cert", "client_cert", "invalid")
        assert square.urlpath(cfg, "Deployment", "valid-ns") == RetVal(None, True)


class TestDownloadManifests:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    @mock.patch.object(k8s, 'get')
    def test_download_manifests_ok(self, m_get):
        """Download two kinds of manifests: Deployments and Namespaces.

        The test only mocks the call to the K8s API. All other functions
        actually execute.

        """
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10")
        mm = make_manifest

        meta = [
            mm("Namespace", None, "ns0"),
            mm("Namespace", None, "ns1"),
            mm("Deployment", "ns0", "d0"),
            mm("Deployment", "ns1", "d1"),
        ]

        # Demo lists as K8s would return them (we will make m_get return them).
        man_list_ns = {
            'apiVersion': 'v1',
            'kind': 'NamespaceList',
            'items': [meta[0], meta[1]],
        }
        man_list_deploy = {
            'apiVersion': 'v1',
            'kind': 'DeploymentList',
            'items': [meta[2], meta[3]],
        }
        m_get.side_effect = [RetVal(man_list_ns, False), RetVal(man_list_deploy, False)]

        # The request URLs. We will need them to validate the `get` arguments.
        url_ns, err1 = square.urlpath(config, "Namespace", None)
        url_deploy, err2 = square.urlpath(config, "Deployment", None)
        assert not err1 and not err2

        # The expected outcome is a Dict[MetaManifest:dict] of all the items in
        # DeploymentList and NamespaceList.
        expected = {
            manio.make_meta(_): manio.metaspec(_).data
            for _ in meta
        }

        # Run test function and verify its output.
        ret = square.download_manifests(
            config, "client", ["Namespace", "Deployment"], None
        )
        assert ret == RetVal(expected, False)
        assert m_get.call_args_list == [
            mock.call("client", url_ns),
            mock.call("client", url_deploy),
        ]

    @mock.patch.object(k8s, 'get')
    def test_download_manifests_err(self, m_get):
        """Simulate a download error."""
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10")

        # A valid NamespaceList with one element.
        man_list_ns = {
            'apiVersion': 'v1',
            'kind': 'NamespaceList',
            'items': [make_manifest("Namespace", None, "ns0")],
        }

        # The first call to get will succeed whereas the second will not.
        m_get.side_effect = [RetVal(man_list_ns, False), RetVal(None, True)]

        # The request URLs. We will need them to validate the `get` arguments.
        url_ns, err1 = square.urlpath(config, "Namespace", None)
        url_deploy, err2 = square.urlpath(config, "Deployment", None)
        assert not err1 and not err2

        # Run test function and verify it returns an error and no data, despite
        # a successful `NamespaceList` download.
        ret = square.download_manifests(
            config, "client", ["Namespace", "Deployment"], None
        )
        assert ret == RetVal(None, True)
        assert m_get.call_args_list == [
            mock.call("client", url_ns),
            mock.call("client", url_deploy),
        ]


class TestPatchK8s:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def test_make_patch_empty(self):
        """Basic test: compute patch between two identical resources."""
        # Setup.
        kind, ns, name = 'Deployment', 'ns', 'foo'
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10")

        # PATCH URLs require the resource name at the end of the request path.
        url = square.urlpath(config, kind, ns).data + f'/{name}'

        # The patch must be empty for identical manifests.
        loc = srv = make_manifest(kind, ns, name)
        ret = square.make_patch(config, loc, srv)
        assert ret == RetVal(Patch(url, []), False)
        assert isinstance(ret.data, Patch)

    def test_make_patch_incompatible(self):
        """Must not try to compute diffs for incompatible manifests.

        For instance, refuse to compute a patch when one manifest has kind
        "Namespace" and the other "Deployment". The same is true for
        "apiVersion", "metadata.name" and "metadata.namespace".

        """
        # Setup.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10")

        # Demo manifest.
        srv = make_manifest('Deployment', 'namespace', 'name')

        # `apiVersion` must match.
        loc = copy.deepcopy(srv)
        loc['apiVersion'] = 'mismatch'
        assert square.make_patch(config, loc, srv) == RetVal(None, True)

        # `kind` must match.
        loc = copy.deepcopy(srv)
        loc['kind'] = 'Mismatch'
        assert square.make_patch(config, loc, srv) == RetVal(None, True)

        # `name` must match.
        loc = copy.deepcopy(srv)
        loc['metadata']['name'] = 'mismatch'
        assert square.make_patch(config, loc, srv) == RetVal(None, True)

        # `namespace` must match.
        loc = copy.deepcopy(srv)
        loc['metadata']['namespace'] = 'mismatch'
        assert square.make_patch(config, loc, srv) == RetVal(None, True)

    def test_make_patch_namespace(self):
        """`Namespace` specific corner cases.

        Namespaces are special because, by definition, they must not contain a
        `metadata.Namespace` attribute.

        """
        config = types.SimpleNamespace(url='http://examples.com/', version="1.10")
        kind, name = 'Namespace', 'foo'

        url = square.urlpath(config, kind, None).data + f'/{name}'

        # Must succeed and return an empty patch for identical manifests.
        loc = srv = make_manifest(kind, None, name)
        assert square.make_patch(config, loc, srv) == RetVal((url, []), False)

        # Second manifest specifies a `metadata.namespace` attribute. This is
        # invalid and must result in an error.
        loc = make_manifest(kind, None, name)
        srv = copy.deepcopy(loc)
        loc['metadata']['namespace'] = 'foo'
        ret = square.make_patch(config, loc, srv)
        assert ret.data is None and ret.err is not None

        # Must not return an error if the input are the same namespace resource
        # but with different labels.
        loc = make_manifest(kind, None, name)
        srv = copy.deepcopy(loc)
        loc['metadata']['labels'] = {"key": "value"}

        ret = square.make_patch(config, loc, srv)
        assert ret.err is False and len(ret.data) > 0


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
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10")

        # Two valid manifests.
        kind, namespace, name = "Deployment", "namespace", "name"
        srv = make_manifest(kind, namespace, name)
        loc = make_manifest(kind, namespace, name)
        srv["metadata"]["labels"] = {"old": "old"}
        loc["metadata"]["labels"] = {"new": "new"}

        # The Patch between two identical manifests must be a No-Op.
        expected = square.Patch(
            url=square.urlpath(config, kind, namespace).data + f"/{name}",
            ops=[],
        )
        assert square.make_patch(config, loc, loc) == RetVal(expected, False)

        # The patch between `srv` and `loc` must remove the old label and add
        # the new one.
        expected = square.Patch(
            url=square.urlpath(config, kind, namespace).data + f"/{name}",
            ops=[
                {'op': 'remove', 'path': '/metadata/labels/old'},
                {'op': 'add', 'path': '/metadata/labels/new', 'value': 'new'}
            ]
        )
        assert square.make_patch(config, loc, srv) == RetVal(expected, False)

    def test_make_patch_err(self):
        """Verify error cases with invalid or incompatible manifests."""
        valid_cfg = k8s.Config("url", "token", "cert", "client_cert", "1.10")
        invalid_cfg = k8s.Config("url", "token", "cert", "client_cert", "invalid")

        # Create two valid manifests, then stunt one in such a way that
        # `metaspec` will reject it.
        kind, namespace, name = "Deployment", "namespace", "name"
        valid = make_manifest(kind, namespace, name)
        invalid = make_manifest(kind, namespace, name)
        del invalid["kind"]

        # Must handle errors from `metaspec`.
        assert square.make_patch(valid_cfg, valid, invalid) == RetVal(None, True)
        assert square.make_patch(valid_cfg, invalid, valid) == RetVal(None, True)
        assert square.make_patch(valid_cfg, invalid, invalid) == RetVal(None, True)

        # Must handle `urlpath` errors.
        assert square.make_patch(invalid_cfg, valid, valid) == RetVal(None, True)

        # Must handle incompatible manifests, ie manifests that do not belong
        # to the same resource.
        valid_a = make_manifest(kind, namespace, "bar")
        valid_b = make_manifest(kind, namespace, "foo")
        assert square.make_patch(valid_cfg, valid_a, valid_b) == RetVal(None, True)

    def test_compile_plan_create_delete_ok(self):
        """Test a plan that creates and deletes resource, but not patches any.

        To do this, the local and server resources are all distinct. As a
        result, the returned plan must dictate that all local resources shall
        be created, all server resources deleted, and none patched.

        """
        # Create vanilla `Config` instance.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10")

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
        upb = square.urlpath
        url[0] = upb(config, meta[0].kind, meta[0].namespace).data
        url[1] = upb(config, meta[1].kind, meta[1].namespace).data

        # Determine the K8s resource URLs for those that will be deleted. They
        # are slightly different because DELETE requests expect a URL path that
        # ends with the resource, eg
        # "/api/v1/namespaces/ns2"
        # instead of
        # "/api/v1/namespaces".
        url[2] = upb(config, meta[2].kind, meta[2].namespace).data + "/" + meta[2].name
        url[3] = upb(config, meta[3].kind, meta[3].namespace).data + "/" + meta[3].name
        url[4] = upb(config, meta[4].kind, meta[4].namespace).data + "/" + meta[4].name

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
        expected = square.DeploymentPlan(
            create=[
                square.DeltaCreate(meta[0], url[0], loc_man[meta[0]]),
                square.DeltaCreate(meta[1], url[1], loc_man[meta[1]]),
            ],
            patch=[],
            delete=[
                square.DeltaDelete(meta[2], url[2], del_opts),
                square.DeltaDelete(meta[3], url[3], del_opts),
                square.DeltaDelete(meta[4], url[4], del_opts),
            ],
        )
        assert square.compile_plan(config, loc_man, srv_man) == RetVal(expected, False)

    @mock.patch.object(square, "partition_manifests")
    def test_compile_plan_create_delete_err(self, m_part):
        """Simulate `urlpath` errors"""
        # Invalid configuration. We will use it to trigger an error in `urlpath`.
        cfg_invalid = k8s.Config("url", "token", "cert", "cert", "invalid")

        # Valid ManifestMeta and dummy manifest dict.
        meta = manio.make_meta(make_manifest("Deployment", "ns", "name"))
        man = {meta: None}

        # Pretend we only have to "create" resources, and then trigger the
        # `urlpath` error in its code path.
        m_part.return_value = RetVal(
            data=square.DeploymentPlan(create=[meta], patch=[], delete=[]),
            err=False,
        )
        assert square.compile_plan(cfg_invalid, man, man) == RetVal(None, True)

        # Pretend we only have to "delete" resources, and then trigger the
        # `urlpath` error in its code path.
        m_part.return_value = RetVal(
            data=square.DeploymentPlan(create=[], patch=[], delete=[meta]),
            err=False,
        )
        assert square.compile_plan(cfg_invalid, man, man) == RetVal(None, True)

    def test_compile_plan_patch_no_diff(self):
        """Test a plan that patches all resources.

        To do this, the local and server resources are identical. As a
        result, the returned plan must nominate all manifests for patching, and
        none to create and delete.

        """
        # Create vanilla `Config` instance.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10")

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
            square.urlpath(config, _.kind, _.namespace).data + f"/{_.name}"
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
        expected = square.DeploymentPlan(
            create=[],
            patch=[
                square.DeltaPatch(meta[0], "", square.Patch(url[0], [])),
                square.DeltaPatch(meta[1], "", square.Patch(url[1], [])),
                square.DeltaPatch(meta[2], "", square.Patch(url[2], [])),
                square.DeltaPatch(meta[3], "", square.Patch(url[3], [])),
            ],
            delete=[]
        )
        assert square.compile_plan(config, loc_man, srv_man) == RetVal(expected, False)

    def test_compile_plan_patch_with_diff(self):
        """Test a plan that patches all resources.

        To do this, the local and server resources are identical. As a
        result, the returned plan must nominate all manifests for patching, and
        none to create and delete.

        """
        # Create vanilla `Config` instance.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10")

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
        diff_str, err = manio.diff(loc_man[meta], srv_man[meta])
        assert not err

        # Verify the test function returns the correct Patch and diff.
        expected = square.DeploymentPlan(
            create=[],
            patch=[square.DeltaPatch(meta, diff_str, patch)],
            delete=[]
        )
        assert square.compile_plan(config, loc_man, srv_man) == RetVal(expected, False)

    @mock.patch.object(square, "partition_manifests")
    @mock.patch.object(manio, "diff")
    @mock.patch.object(square, "make_patch")
    def test_compile_plan_err(self, m_patch, m_diff, m_part):
        """Use mocks for the internal function calls to simulate errors."""
        # Create vanilla `Config` instance.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10")

        # Define a single resource and valid dummy return value for
        # `square.partition_manifests`.
        meta = MetaManifest('v1', 'Namespace', None, 'ns1')
        plan = square.DeploymentPlan(create=[], patch=[meta], delete=[])

        # Local and server manifests have the same resources but their
        # definition differs. This will ensure a non-empty patch in the plan.
        loc_man = srv_man = {meta: make_manifest("Namespace", None, "ns1")}

        # Simulate an error in `compile_plan`.
        m_part.return_value = RetVal(None, True)
        assert square.compile_plan(config, loc_man, srv_man) == RetVal(None, True)

        # Simulate an error in `diff`.
        m_part.return_value = RetVal(plan, False)
        m_diff.return_value = RetVal(None, True)
        assert square.compile_plan(config, loc_man, srv_man) == RetVal(None, True)

        # Simulate an error in `make_patch`.
        m_part.return_value = RetVal(plan, False)
        m_diff.return_value = RetVal("some string", False)
        m_patch.return_value = RetVal(None, True)
        assert square.compile_plan(config, loc_man, srv_man) == RetVal(None, True)


class TestLocalServerManfiests:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    @mock.patch.object(manio, "load")
    @mock.patch.object(manio, "unpack")
    @mock.patch.object(square, "download_manifests")
    def test_get_manifests(self, m_download, m_unpack, m_load):
        """Basic test.

        This function does hardly anything to begin with, so we will merely
        verify it calls the correct functions with the correct arguments and
        handles errors correctly.

        """
        # Pretend that all auxiliary functions succeed.
        m_load.return_value = RetVal("fdata_meta", False)
        m_unpack.return_value = RetVal("loc", False)
        m_download.return_value = RetVal("srv", False)

        # The arguments to the test function will always be the same in this test.
        args = "config", "client", "folder", "kinds", "ns"

        fun = square.get_manifests
        assert fun(*args) == RetVal(("loc", "srv", "fdata_meta"), False)
        m_load.assert_called_once_with("folder")
        m_unpack.assert_called_once_with("fdata_meta")
        m_download.assert_called_once_with("config", "client", "kinds", "ns")

        # Simulate an error with `manio.load`.
        m_load.return_value = RetVal(None, True)
        assert fun(*args) == RetVal(None, True)

        # Simulate an error with `manio.unpack`.
        m_unpack.return_value = RetVal(None, True)
        assert fun(*args) == RetVal(None, True)

        # Simulate an error with `square.download_manifests`.
        m_download.return_value = RetVal(None, True)
        assert fun(*args) == RetVal(None, True)


class TestMain:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    @mock.patch.object(square, "get_manifests")
    @mock.patch.object(square, "compile_plan")
    @mock.patch.object(k8s, "post")
    @mock.patch.object(k8s, "patch")
    @mock.patch.object(k8s, "delete")
    def test_main_patch(self, m_delete, m_patch, m_post, m_plan, m_lsm):
        """Simulate a successful resource update (add, patch delete).

        To this end, create a valid (mocked) deployment plan, mock out all
        * calls, and verify that all the necessary calls are made.

        The second part of the test simulates errors. This is not a separate
        test because it shares virtually all the boiler plate code.
        """
        # Valid client config and MetaManifest.
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10")
        meta = manio.make_meta(make_manifest("Deployment", "ns", "name"))

        # Valid Patch.
        patch = square.Patch(
            url="patch_url",
            ops=[
                {'op': 'remove', 'path': '/metadata/labels/old'},
                {'op': 'add', 'path': '/metadata/labels/new', 'value': 'new'}
            ],
        )

        # Valid deployment plan.
        plan = square.DeploymentPlan(
            create=[square.DeltaCreate(meta, "create_url", "create_man")],
            patch=[square.DeltaPatch(meta, "diff", patch)],
            delete=[square.DeltaDelete(meta, "delete_url", "delete_man")],
        )

        # Pretend that all K8s requests succeed.
        m_lsm.return_value = RetVal(square.Manifests("loc", "srv", "files"), False)
        m_plan.return_value = RetVal(plan, False)
        m_post.return_value = RetVal(None, False)
        m_patch.return_value = RetVal(None, False)
        m_delete.return_value = RetVal(None, False)

        # The arguments to the test function will always be the same in this test.
        args = config, "client", "folder", "kinds", "ns"

        # Update the K8s resources and verify that the test functions made the
        # corresponding calls to K8s.
        assert square.main_patch(*args) == RetVal(None, False)
        m_lsm.assert_called_once_with(*args)
        m_plan.assert_called_once_with(config, "loc", "srv")
        m_post.assert_called_once_with("client", "create_url", "create_man")
        m_patch.assert_called_once_with("client", patch.url, patch.ops)
        m_delete.assert_called_once_with("client", "delete_url", "delete_man")

        # -----------------------------------------------------------------
        #                   Simulate Error Scenarios
        # -----------------------------------------------------------------
        # Make `delete` fail.
        m_delete.return_value = RetVal(None, True)
        assert square.main_patch(*args) == RetVal(None, True)

        # Make `patch` fail.
        m_patch.return_value = RetVal(None, True)
        assert square.main_patch(*args) == RetVal(None, True)

        # Make `post` fail.
        m_post.return_value = RetVal(None, True)
        assert square.main_patch(*args) == RetVal(None, True)

        # Make `compile_plan` fail.
        m_plan.return_value = RetVal(None, True)
        assert square.main_patch(*args) == RetVal(None, True)

        # Make `get_manifests` fail.
        m_lsm.return_value = RetVal(None, True)
        assert square.main_patch(*args) == RetVal(None, True)

    @mock.patch.object(square, "get_manifests")
    @mock.patch.object(square, "compile_plan")
    def test_main_diff(self, m_plan, m_lsm):
        """Basic test.

        This function does hardly anything to begin with, so we will merely
        verify it calls the correct functions with the correct arguments and
        handles errors correctly.

        """
        # Valid deployment plan.
        plan = square.DeploymentPlan(create=[], patch=[], delete=[])

        # All auxiliary functions will succeed.
        m_lsm.return_value = RetVal(square.Manifests("loc", "srv", "files"), False)
        m_plan.return_value = RetVal(plan, False)

        # The arguments to the test function will always be the same in this test.
        args = "config", "client", "folder", "kinds", "ns"

        # A successfull DIFF only computes and prints the plan.
        assert square.main_diff(*args) == RetVal(None, False)
        m_lsm.assert_called_once_with(*args)
        m_plan.assert_called_once_with("config", "loc", "srv")

        # Simulate an error in `compute_plan`.
        m_plan.return_value = RetVal(None, True)
        assert square.main_diff(*args) == RetVal(None, True)

        # Simulate an error in `get_manifests`.
        m_lsm.return_value = RetVal(None, True)
        assert square.main_diff(*args) == RetVal(None, True)

    @mock.patch.object(square, "get_manifests")
    @mock.patch.object(manio, "sync")
    @mock.patch.object(manio, "save")
    def test_main_get(self, m_save, m_sync, m_lsm):
        """Basic test.

        This function does hardly anything to begin with, so we will merely
        verify it calls the correct functions with the correct arguments and
        handles errors correctly.

        """
        # Simulate successful responses from the two auxiliary functions.
        m_lsm.return_value = RetVal(square.Manifests("loc", "srv", "files"), False)
        m_sync.return_value = RetVal("synced", False)
        m_save.return_value = RetVal(None, False)

        # The arguments to the test function will always be the same in this test.
        args = "config", "client", "folder", "kinds", "ns"

        # Call test function and verify it passed the correct arguments.
        assert square.main_get(*args) == RetVal(None, False)
        m_lsm.assert_called_once_with(*args)
        m_sync.assert_called_once_with("files", "srv")
        m_save.assert_called_once_with("folder", "synced")

        # Simulate an error with `manio.save`.
        m_save.return_value = RetVal(None, True)
        assert square.main_get(*args) == RetVal(None, True)

        # Simulate an error with `manio.sync`.
        m_sync.return_value = RetVal(None, True)
        assert square.main_get(*args) == RetVal(None, True)

        # Simulate an error in `get_manifests`.
        m_lsm.return_value = RetVal(None, True)
        assert square.main_get(*args) == RetVal(None, True)
