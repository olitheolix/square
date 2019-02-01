import copy
import os
import random
import types
import unittest.mock as mock
import k8s_utils

import requests_mock

import pytest

import square

# Convenience.
pjoin = os.path.join
RetVal, DeploymentPlan = square.RetVal, square.DeploymentPlan
MetaManifest = square.MetaManifest
Patch = square.Patch
requests = k8s_utils.requests


@pytest.fixture
def m_requests(request):
    with requests_mock.Mocker() as m:
        yield m


def make_manifest(kind: str, namespace: str, name: str):
    manifest = {
        'apiVersion': 'v1',
        'kind': kind,
        'metadata': {
            'name': name,
            'labels': {'key': 'val'},
            'foo': 'bar',
        },
        'spec': {
            'finalizers': ['kubernetes']
        },
        'status': {
            'some': 'status',
        },
        'garbage': 'more garbage',
    }

    # Only create namespace entry if one was specified.
    if namespace is not None:
        manifest['metadata']['namespace'] = namespace

    return manifest


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


class TestManifestValidation:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def test_manifest_metaspec_basic_valid(self):
        """Ensure it returns only the salient fields of valid manifests."""

        # Minimal Deployment manifest with just the salient fields. Test
        # function must return a deepcopy of it.
        valid_deployment_manifest = {
            'apiVersion': 'v1',
            'kind': 'Deployment',
            'metadata': {'name': 'foo', 'namespace': 'bar'},
            'spec': {'some': 'thing'},
        }
        ret = square.manifest_metaspec(valid_deployment_manifest)
        assert ret == RetVal(valid_deployment_manifest, False)
        assert valid_deployment_manifest is not ret.data

        # Minimal Namespace manifest with just the salient fields. Test
        # function must return a deepcopy of it.
        valid_namespace_manifest = {
            'apiVersion': 'v1',
            'kind': 'Namespace',
            'metadata': {'name': 'foo'},
            'spec': {'some': 'thing'},
        }
        ret = square.manifest_metaspec(valid_namespace_manifest)
        assert ret == RetVal(valid_namespace_manifest, False)
        assert valid_namespace_manifest is not ret.data

        # Function must accept additional entries (eg "additional" in example
        # below) but not return them. It must not matter whether those
        # additional entries are actually valid keys in the manifest.
        valid_namespace_manifest_add = {
            'apiVersion': 'v1',
            'kind': 'Namespace',
            'metadata': {'name': 'foo', 'additional': 'entry'},
            'spec': {'some': 'thing'},
            'status': {"some": "status"},
            'additional': 'entry',
        }
        ret = square.manifest_metaspec(valid_namespace_manifest_add)
        assert ret == RetVal(valid_namespace_manifest, False)

    def test_manifest_metaspec_automanifests(self):
        """Verify that it works with the `make_manifest` test function.

        This test merely validates that the output of the `make_manifest`
        function used in various tests produces valid manifests as far as
        `manifest_metaspec` is concerned.

        """
        # Create a valid manifest for each supported resource kind and verify
        # that the test function accepts it.
        for kind in square.SUPPORTED_KINDS:
            if kind == "Namespace":
                manifest = make_manifest(kind, None, "name")
            else:
                manifest = make_manifest(kind, "ns", "name")

            ret = square.manifest_metaspec(manifest)
            assert ret.err is False and len(ret.data) > 0

        # Invalid Namespace manifest: metadata.namespace field is not None.
        manifest = make_manifest("Namespace", "ns", "name")
        assert square.manifest_metaspec(manifest) == RetVal(None, True)

        # Unknown resource kind "foo".
        manifest = make_manifest("foo", "ns", "name")
        assert square.manifest_metaspec(manifest) == RetVal(None, True)

    def test_manifest_metaspec_missing_fields(self):
        """Incomplete manifests must be rejected."""
        # A valid deployment manifest.
        valid = {
            "apiVersion": "v1",
            "kind": "Deployment",
            "metadata": {"name": "foo", "namespace": "bar"},
            "spec": {"some": "thing"},
        }

        # Create stunted manifests by creating a copy of `valid` that misses
        # one key in each iteration. The test function must reject all those
        # manifests and return an error.
        for field in valid:
            invalid = {k: v for k, v in valid.items() if k != field}
            assert square.manifest_metaspec(invalid) == RetVal(None, True)

        # Metadata for Namespace manifests must contain a "name" field.
        invalid = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"foo": "bar"},
            "spec": {"some": "thing"},
        }
        assert square.manifest_metaspec(invalid) == RetVal(None, True)

        # Metadata for Namespace manifests must not contain a "namespace" field.
        invalid = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"namespace": "namespace"},
            "spec": {"some": "thing"},
        }
        assert square.manifest_metaspec(invalid) == RetVal(None, True)

        # Metadata for non-namespace manifests must contain "name" and "namespace".
        invalid = {
            "apiVersion": "v1",
            "kind": "Deployment",
            "metadata": {"name": "name"},
            "spec": {"some": "thing"},
        }
        assert square.manifest_metaspec(invalid) == RetVal(None, True)


class TestFetchFromK8s:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def test_list_parser_ok(self):
        """Convert eg a DeploymentList into a Python dict of Deployments."""
        # Demo manifests.
        manifests = [
            make_manifest('Deployment', f'ns_{_}', f'name_{_}')
            for _ in range(3)
        ]

        # The actual DeploymentList returned from K8s.
        manifest_list = {
            'apiVersion': 'v1',
            'kind': 'DeploymentList',
            'items': manifests,
        }

        # Parse the DeploymentList into a dict. The keys are ManifestTuples and
        # the values are the Deployment (*not* DeploymentList) manifests.
        ret = square.list_parser(manifest_list)
        assert ret.err is False

        # Verify the Python dict.
        assert len(manifests) == 3
        assert ret.data == {
            MetaManifest('v1', 'Deployment', 'ns_0', 'name_0'): manifests[0],
            MetaManifest('v1', 'Deployment', 'ns_1', 'name_1'): manifests[1],
            MetaManifest('v1', 'Deployment', 'ns_2', 'name_2'): manifests[2],
        }

        # Function must return deep copies of the manifests to avoid difficult
        # to debug reference bugs.
        for src, out_key in zip(manifests, ret.data):
            assert src == ret.data[out_key]
            assert src is not ret.data[out_key]

    def test_list_parser_invalid_list_manifest(self):
        """The input manifest must have `apiVersion`, `kind` and `items`.

        Furthermore, the `kind` *must* be capitalised and end in `List`, eg
        `DeploymentList`.

        """
        # Valid input.
        src = {'apiVersion': 'v1', 'kind': 'DeploymentList', 'items': []}
        ret = square.list_parser(src)
        assert ret == RetVal({}, False)

        # Missing `apiVersion`.
        src = {'kind': 'DeploymentList', 'items': []}
        assert square.list_parser(src) == RetVal(None, True)

        # Missing `kind`.
        src = {'apiVersion': 'v1', 'items': []}
        assert square.list_parser(src) == RetVal(None, True)

        # Missing `items`.
        src = {'apiVersion': 'v1', 'kind': 'DeploymentList'}
        assert square.list_parser(src) == RetVal(None, True)

        # All fields present but `kind` does not end in List (case sensitive).
        for invalid_kind in ('Deploymentlist', 'Deployment'):
            src = {'apiVersion': 'v1', 'kind': invalid_kind, 'items': []}
            assert square.list_parser(src) == RetVal(None, True)


class TestK8sDeleteGetPatchPost:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    def test_k8s_request_ok(self, method, m_requests):
        """Simulate a successful K8s response for GET request."""
        # Dummy values for the K8s API request.
        url = 'http://examples.com/'
        client = requests.Session()
        headers = {"some": "headers"}
        payload = {"some": "payload"}
        response = {"some": "response"}

        # Verify the makeup of the actual request.
        def additional_matcher(req):
            assert req.method == method
            assert req.url == url
            assert req.json() == payload
            assert req.headers["some"] == headers["some"]
            assert req.timeout == 30
            return True

        # Assign a random HTTP status code.
        status_code = random.randint(100, 510)
        m_requests.request(
            method,
            url,
            json=response,
            status_code=status_code,
            additional_matcher=additional_matcher,
        )

        # Verify that the function makes the correct request and returns the
        # expected result and HTTP status code.
        ret = square.k8s_request(client, method, url, payload, headers)
        assert ret == RetVal(response, status_code)

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    def test_k8s_request_err_json(self, method, m_requests):
        """Simulate a corrupt JSON response from K8s."""
        # Dummies for K8s API URL and `requests` session.
        url = 'http://examples.com/'
        client = requests.Session()

        # Construct a response with a corrupt JSON string.
        corrupt_json = "{this is not valid] json;"
        m_requests.request(
            method,
            url,
            text=corrupt_json,
            status_code=200,
        )

        # Test function must not return a response but indicate an error.
        ret = square.k8s_request(client, method, url, None, None)
        assert ret == RetVal(None, True)

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    def test_k8s_request_connection_err(self, method, m_requests):
        """Simulate an unsuccessful K8s response for GET request."""
        # Dummies for K8s API URL and `requests` session.
        url = 'http://examples.com/'
        client = requests.Session()

        # Construct the ConnectionError exception with a fake request object.
        # The fake is necessary to ensure that the exception handler extracts
        # the correct pieces of information from it.
        req = types.SimpleNamespace(method=method, url=url)
        exc = k8s_utils.requests.exceptions.ConnectionError(request=req)

        # Simulate a connection error during the request to K8s.
        m_requests.request(method, url, exc=exc)
        ret = square.k8s_request(client, method, url, None, None)
        assert ret == RetVal(None, True)

    @mock.patch.object(square, "k8s_request")
    def test_k8s_delete_get_patch_post_ok(self, m_req):
        """Simulate successful DELETE, GET, PATCH, POST requests.

        This test is for the various wrappers around the `k8s_request`
        function, which is why we mock it so as to return an HTTP code that
        constitutes a successful transaction for the respective request.

        """
        # Dummy values.
        client = "client"
        path = "path"
        payload = "payload"
        response = "response"

        # K8s DELETE request was successful iff its return status is 200.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 200)
        assert square.k8s_delete(client, path, payload) == RetVal(response, False)
        m_req.assert_called_once_with(client, "DELETE", path, payload, headers=None)

        # K8s GET request was successful iff its return status is 200.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 200)
        assert square.k8s_get(client, path) == RetVal(response, False)
        m_req.assert_called_once_with(client, "GET", path, payload=None, headers=None)

        # K8s PATCH request was successful iff its return status is 200.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 200)
        assert square.k8s_patch(client, path, payload) == RetVal(response, False)
        patch_headers = {'Content-Type': 'application/json-patch+json'}
        m_req.assert_called_once_with(client, "PATCH", path, payload, patch_headers)

        # K8s POST request was successful iff its return status is 201.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 201)
        assert square.k8s_post(client, path, payload) == RetVal(response, False)
        m_req.assert_called_once_with(client, "POST", path, payload, headers=None)

    @mock.patch.object(square, "k8s_request")
    def test_k8s_delete_get_patch_post_err(self, m_req):
        """Simulate unsuccessful DELETE, GET, PATCH, POST requests.

        This test is for the various wrappers around the `k8s_request`
        function, which is why we mock it so as to return an HTTP code that
        constitutes an unsuccessful transaction for the respective request.

        """
        # Dummy values.
        client = "client"
        path = "path"
        payload = "payload"
        response = "response"

        # K8s DELETE request was unsuccessful because its returns status is not 200.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 400)
        assert square.k8s_delete(client, path, payload) == RetVal(response, True)
        m_req.assert_called_once_with(client, "DELETE", path, payload, headers=None)

        # K8s GET request was unsuccessful because its returns status is not 200.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 400)
        assert square.k8s_get(client, path) == RetVal(response, True)
        m_req.assert_called_once_with(client, "GET", path, payload=None, headers=None)

        # K8s PATCH request was unsuccessful because its returns status is not 200.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 400)
        assert square.k8s_patch(client, path, payload) == RetVal(response, True)
        patch_headers = {'Content-Type': 'application/json-patch+json'}
        m_req.assert_called_once_with(client, "PATCH", path, payload, patch_headers)

        # K8s POST request was unsuccessful because its returns status is not 201.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 400)
        assert square.k8s_post(client, path, payload) == RetVal(response, True)
        m_req.assert_called_once_with(client, "POST", path, payload, headers=None)


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
        Config = k8s_utils.Config
        for version in square.SUPPORTED_VERSIONS:
            cfg = Config("url", "token", "ca_cert", "client_cert", version)
            for kind in square.SUPPORTED_KINDS:
                for ns in (None, "foo-namespace"):
                    path, err = square.urlpath(cfg, kind, ns)

                # Verify.
                assert err is False
                assert isinstance(path, str)

    def test_urlpath_err(self):
        """Test variuos error scenarios."""
        Config = k8s_utils.Config

        for version in square.SUPPORTED_VERSIONS:
            cfg = Config("url", "token", "ca_cert", "client_cert", version)

            # Invalid resource kind.
            assert square.urlpath(cfg, "fooresource", "ns") == RetVal(None, True)

            # Namespace names must be all lower case (K8s imposes this)...
            assert square.urlpath(cfg, "Deployment", "namEspACe") == RetVal(None, True)


class TestPatchK8s:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def test_compute_patch_empty(self):
        """Basic test: compute patch between two identical resources."""
        # Setup.
        kind, ns, name = 'Deployment', 'ns', 'foo'
        config = k8s_utils.Config("url", "token", "ca_cert", "client_cert", "1.10")

        # PATCH URLs require the resource name at the end of the request path.
        url = square.urlpath(config, kind, ns).data + f'/{name}'

        # The patch must be empty for identical manifests.
        loc = srv = make_manifest(kind, ns, name)
        ret = square.compute_patch(config, loc, srv)
        assert ret == RetVal(Patch(url, []), False)
        assert isinstance(ret.data, Patch)

    def test_compute_patch_incompatible(self):
        """Must not try to compute diffs for incompatible manifests.

        For instance, refuse to compute a patch when one manifest has kind
        "Namespace" and the other "Deployment". The same is true for
        "apiVersion", "metadata.name" and "metadata.namespace".

        """
        # Setup.
        config = k8s_utils.Config("url", "token", "ca_cert", "client_cert", "1.10")

        # Demo manifest.
        srv = make_manifest('Deployment', 'namespace', 'name')

        # `apiVersion` must match.
        loc = copy.deepcopy(srv)
        loc['apiVersion'] = 'mismatch'
        assert square.compute_patch(config, loc, srv) == RetVal(None, True)

        # `kind` must match.
        loc = copy.deepcopy(srv)
        loc['kind'] = 'Mismatch'
        assert square.compute_patch(config, loc, srv) == RetVal(None, True)

        # `name` must match.
        loc = copy.deepcopy(srv)
        loc['metadata']['name'] = 'mismatch'
        assert square.compute_patch(config, loc, srv) == RetVal(None, True)

        # `namespace` must match.
        loc = copy.deepcopy(srv)
        loc['metadata']['namespace'] = 'mismatch'
        assert square.compute_patch(config, loc, srv) == RetVal(None, True)

    def test_compute_patch_namespace(self):
        """`Namespace` specific corner cases.

        Namespaces are special because, by definition, they must not contain a
        `metadata.Namespace` attribute.

        """
        config = types.SimpleNamespace(url='http://examples.com/', version="1.10")
        kind, name = 'Namespace', 'foo'

        url = square.urlpath(config, kind, None).data + f'/{name}'

        # Must succeed and return an empty patch for identical manifests.
        loc = srv = make_manifest(kind, None, name)
        assert square.compute_patch(config, loc, srv) == RetVal((url, []), False)

        # Second manifest specifies a `metadata.namespace` attribute. This is
        # invalid and must result in an error.
        loc = make_manifest(kind, None, name)
        srv = copy.deepcopy(loc)
        loc['metadata']['namespace'] = 'foo'
        ret = square.compute_patch(config, loc, srv)
        assert ret.data is None and ret.err is not None

        # Must not return an error if the input are the same namespace resource
        # but with different labels.
        loc = make_manifest(kind, None, name)
        srv = copy.deepcopy(loc)
        loc['metadata']['labels'] = {"key": "value"}

        ret = square.compute_patch(config, loc, srv)
        assert ret.err is False and len(ret.data) > 0


class TestK8sConfig:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    @mock.patch.object(square, "k8s_get")
    def test_get_k8s_version_auto(self, m_get):
        """Get K8s version number from API server."""

        # This is a genuine K8s response from Minikube.
        response = {
            'major': '1', 'minor': '10',
            'gitVersion': 'v1.10.0',
            'gitCommit': 'fc32d2f3698e36b93322a3465f63a14e9f0eaead',
            'gitTreeState': 'clean',
            'buildDate': '2018-03-26T16:44:10Z',
            'goVersion': 'go1.9.3',
            'compiler': 'gc', 'platform': 'linux/amd64'
        }
        m_get.return_value = RetVal(response, None)

        # Create vanilla `Config` instance.
        m_client = mock.MagicMock()
        config = k8s_utils.Config("url", "token", "ca_cert", "client_cert", None)

        # Test function must contact the K8s API and return a `Config` tuple
        # with the correct version number.
        config2, err = square.get_k8s_version(config, client=m_client)
        assert err is None
        assert isinstance(config2, k8s_utils.Config)
        assert config2.version == "1.10"

        # Test function must have called out to `k8s_get` to retrieve the
        # version. Here we ensure it called the correct URL.
        m_get.assert_called_once_with(m_client, f"{config.url}/version")
        assert not m_client.called

        # The return `Config` tuple must be identical to the input except for
        # the version number.
        assert config._replace(version=None) == config2._replace(version=None)


class TestPlan:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def test_compile_plan_create_delete(self):
        """Test a plan that creates and deletes resource, but not patches any.

        To do this, the local and server resources are all distinct. As a
        result, the returned plan must dictate that all local resources shall
        be created, all server resources deleted, and none patched.

        """
        # Create vanilla `Config` instance.
        config = k8s_utils.Config("url", "token", "ca_cert", "client_cert", "1.10")

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

    def test_compile_plan_patch_no_diff(self):
        """Test a plan that patches all resources.

        To do this, the local and server resources are identical. As a
        result, the returned plan must nominate all manifests for patching, and
        none to create and delete.

        """
        # Create vanilla `Config` instance.
        config = k8s_utils.Config("url", "token", "ca_cert", "client_cert", "1.10")

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
        config = k8s_utils.Config("url", "token", "ca_cert", "client_cert", "1.10")

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
        patch, err = square.compute_patch(config, loc_man[meta], srv_man[meta])
        assert not err
        diff_str, err = square.diff_manifests(srv_man[meta], loc_man[meta])
        assert not err

        # Verify the test function returns the correct Patch and diff.
        expected = square.DeploymentPlan(
            create=[],
            patch=[square.DeltaPatch(meta, diff_str, patch)],
            delete=[]
        )
        assert square.compile_plan(config, loc_man, srv_man) == RetVal(expected, False)

    @mock.patch.object(square, "partition_manifests")
    @mock.patch.object(square, "diff_manifests")
    @mock.patch.object(square, "compute_patch")
    def test_compile_plan_err(self, m_patch, m_diff, m_part):
        """Use mocks for the internal function calls to simulate errors."""
        # Create vanilla `Config` instance.
        config = k8s_utils.Config("url", "token", "ca_cert", "client_cert", "1.10")

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

        # Simulate an error in `diff_manifests`.
        m_part.return_value = RetVal(plan, False)
        m_diff.return_value = RetVal(None, True)
        assert square.compile_plan(config, loc_man, srv_man) == RetVal(None, True)

        # Simulate an error in `compute_patch`.
        m_part.return_value = RetVal(plan, False)
        m_diff.return_value = RetVal("some string", False)
        m_patch.return_value = RetVal(None, True)
        assert square.compile_plan(config, loc_man, srv_man) == RetVal(None, True)
