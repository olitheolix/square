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
        pass

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
        fun = square.partition_manifests
        Plan = square.DeploymentPlan

        # No change because local and cluster manifests are identical.
        local_man = {
            MetaManifest('v1', 'Deployment', 'ns1', 'foo'),
            MetaManifest('v1', 'Deployment', 'ns2', 'bar'),
            MetaManifest('v1', 'Namespace', None, 'ns1'),
            MetaManifest('v1', 'Namespace', None, 'ns2'),
            MetaManifest('v1', 'Namespace', None, 'ns3'),
        }
        cluster_man = local_man
        plan = Plan(create=set(), patch=local_man, delete=set())
        assert fun(local_man, cluster_man) == RetVal(plan, None)

    def test_partition_manifests_add_delete(self):
        """Local and server manifests are orthogonal sets.

        This must produce a plan where all local resources will be created, all
        cluster resources deleted and none patched.

        """
        fun = square.partition_manifests
        Plan = square.DeploymentPlan

        # Local and cluster manifests are orthogonal.
        local_man = {
            MetaManifest('v1', 'Deployment', 'ns2', 'bar'),
            MetaManifest('v1', 'Namespace', None, 'ns2'),
        }
        cluster_man = {
            MetaManifest('v1', 'Deployment', 'ns1', 'foo'),
            MetaManifest('v1', 'Namespace', None, 'ns1'),
            MetaManifest('v1', 'Namespace', None, 'ns3'),
        }
        plan = Plan(
            create={
                MetaManifest('v1', 'Deployment', 'ns2', 'bar'),
                MetaManifest('v1', 'Namespace', None, 'ns2'),
            },
            patch=set(),
            delete={
                MetaManifest('v1', 'Deployment', 'ns1', 'foo'),
                MetaManifest('v1', 'Namespace', None, 'ns1'),
                MetaManifest('v1', 'Namespace', None, 'ns3'),
            }
        )
        assert fun(local_man, cluster_man) == RetVal(plan, None)

    def test_partition_manifests_patch_delete(self):
        """Create plan with resources to delete and patch.

        The local manifests are a strict subset of the cluster. The deployment
        plan must therefore not create any resources, delete everything absent
        from the local manifests and mark the rest for patching.

        """
        fun = square.partition_manifests
        Plan = square.DeploymentPlan

        local_man = {
            MetaManifest('v1', 'Deployment', 'ns2', 'bar1'),
            MetaManifest('v1', 'Namespace', None, 'ns2'),
        }
        cluster_man = {
            MetaManifest('v1', 'Deployment', 'ns1', 'foo'),
            MetaManifest('v1', 'Deployment', 'ns2', 'bar1'),
            MetaManifest('v1', 'Deployment', 'ns2', 'bar2'),
            MetaManifest('v1', 'Namespace', None, 'ns1'),
            MetaManifest('v1', 'Namespace', None, 'ns2'),
            MetaManifest('v1', 'Namespace', None, 'ns3'),
        }
        plan = Plan(
            create=set(),
            patch={
                MetaManifest('v1', 'Deployment', 'ns2', 'bar1'),
                MetaManifest('v1', 'Namespace', None, 'ns2'),
            },
            delete={
                MetaManifest('v1', 'Deployment', 'ns1', 'foo'),
                MetaManifest('v1', 'Deployment', 'ns2', 'bar2'),
                MetaManifest('v1', 'Namespace', None, 'ns1'),
                MetaManifest('v1', 'Namespace', None, 'ns3'),
            }
        )
        assert fun(local_man, cluster_man) == RetVal(plan, None)


class TestManifestValidation:
    def test_manifest_metaspec_basic_valid(self):
        valid_deployment_manifest = {
            'apiVersion': 'v1',
            'kind': 'Deployment',
            'metadata': {'name': 'foo', 'namespace': 'bar'},
            'spec': {'some': 'thing'},
        }
        ret = square.manifest_metaspec(valid_deployment_manifest)
        assert ret == RetVal(valid_deployment_manifest, None)
        assert valid_deployment_manifest is not ret.data

        valid_namespace_manifest = {
            'apiVersion': 'v1',
            'kind': 'Namespace',
            'metadata': {'name': 'foo'},
            'spec': {'some': 'thing'},
        }
        ret = square.manifest_metaspec(valid_namespace_manifest)
        assert ret == RetVal(valid_namespace_manifest, None)
        assert valid_namespace_manifest is not ret.data

        # Function must accept additional entries but not return them.
        valid_namespace_manifest_add = {
            'apiVersion': 'v1',
            'kind': 'Namespace',
            'metadata': {'name': 'foo', 'additional': 'entry'},
            'spec': {'some': 'thing'},
            'additional': 'entry',
        }
        ret = square.manifest_metaspec(valid_namespace_manifest_add)
        assert ret == RetVal(valid_namespace_manifest, None)

    def test_manifest_metaspec_basic_invalid(self):
        ret = square.manifest_metaspec({'invalid': 'manifest'})
        assert ret == RetVal(None, True)

        # Namespace manifest must not have a `metadata.namespace` attribute.
        invalid_manifest = {
            'apiVersion': 'v1',
            'kind': 'Namespace',
            'metadata': {'name': 'foo', 'namespace': 'bar'},
            'spec': {'some': 'thing'},
        }
        assert square.manifest_metaspec(invalid_manifest) == RetVal(None, True)

        # The `kind` attribute must be all lower case with a capital first letter.
        invalid_manifest = {
            'apiVersion': 'v1',
            'kind': 'deployment',
            'metadata': {'name': 'foo', 'namespace': 'bar'},
            'spec': {'some': 'thing'},
        }
        assert square.manifest_metaspec(invalid_manifest) == RetVal(None, True)

        # Same test again: the `kind` attribute must be all lower case with a
        # capital first letter.
        invalid_manifest = {
            'apiVersion': 'v1',
            'kind': 'dePLOYment',
            'metadata': {'name': 'foo', 'namespace': 'bar'},
            'spec': {'some': 'thing'},
        }
        assert square.manifest_metaspec(invalid_manifest) == RetVal(None, True)

    def test_manifest_metaspec_automanifests(self):
        manifest = make_manifest('Deployment', f'ns_0', f'name_0')
        ret = square.manifest_metaspec(manifest)
        assert ret.err is None

        manifest = make_manifest('Namespace', None, f'name_0')
        ret = square.manifest_metaspec(manifest)
        assert ret.err is None

        manifest = make_manifest('Namespace', "ns", f'name_0')
        assert square.manifest_metaspec(manifest) == RetVal(None, True)

    def test_manifest_metaspec_missing_fields(self):
        valid_deployment_manifest = {
            'apiVersion': 'v1',
            'kind': 'Deployment',
            'metadata': {'name': 'foo', 'namespace': 'bar'},
            'spec': {'some': 'thing'},
        }

        for field in valid_deployment_manifest:
            invalid = copy.deepcopy(valid_deployment_manifest)
            invalid.pop(field)
            ret = square.manifest_metaspec(invalid)
            assert ret == RetVal(None, True)

        # Metadata must contain at least 'name' field.
        invalid = copy.deepcopy(valid_deployment_manifest)
        del invalid['metadata']['name']
        assert square.manifest_metaspec(invalid) == RetVal(None, True)


class TestFetchFromK8s:
    def test_list_parser_ok(self):
        """Convert eg a DeploymentList into a Python dict of Deployments."""
        # Demo manifests.
        manifests = [
            make_manifest('Deployment',  f'ns_{_}', f'name_{_}')
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


class TestPatchK8s:
    def test_compute_patch_empty(self):
        """Basic test: compute patch between two identical resources."""
        config = types.SimpleNamespace(url='http://examples.com/', version="1.10")
        kind, ns, name = 'Deployment', 'ns', 'foo'

        url = square.urlpath_builder(config, kind, ns) + f'/{name}'

        loc = srv = make_manifest(kind, ns, name)
        ret = square.compute_patch(config, loc, srv)
        assert ret == RetVal(Patch(url, []), None)
        assert isinstance(ret.data, Patch)

    def test_compute_patch_incompatible(self):
        config = types.SimpleNamespace(url='http://examples.com/')
        kind, ns, name = 'Deployment', 'ns', 'foo'

        srv = make_manifest(kind, ns, name)

        # `apiVersion` must match.
        loc = copy.deepcopy(srv)
        loc['apiVersion'] = 'mismatch'
        ret = square.compute_patch(config, loc, srv)
        assert ret == RetVal(None, True)

        # `kind` must match.
        loc = copy.deepcopy(srv)
        loc['kind'] = 'Mismatch'
        ret = square.compute_patch(config, loc, srv)
        assert ret == RetVal(None, True)

        # `name` must match.
        loc = copy.deepcopy(srv)
        loc['metadata']['name'] = 'mismatch'
        ret = square.compute_patch(config, loc, srv)
        assert ret == RetVal(None, True)

        # `namespace` must match.
        loc = copy.deepcopy(srv)
        loc['metadata']['namespace'] = 'mismatch'
        ret = square.compute_patch(config, loc, srv)
        assert ret == RetVal(None, True)

    def test_compute_patch_namespace(self):
        """Corner cases that are specific to `Namespace` resources.

        Namespaces are special because, by definition, they must not contain a
        `Namespace` attribute.
        """
        config = types.SimpleNamespace(url='http://examples.com/', version="1.10")
        kind, name = 'Namespace', 'foo'

        url = square.urlpath_builder(config, kind, None) + f'/{name}'

        # Identical namespace manifests.
        loc = make_manifest(kind, None, name)
        srv = copy.deepcopy(loc)
        ret = square.compute_patch(config, loc, srv)
        assert ret == RetVal((url, []), None)

        # Second manifest specifies a `metadata.namespace` attribute. This is
        # invalid and must result in an error.
        loc = make_manifest(kind, None, name)
        srv = copy.deepcopy(loc)
        loc['metadata']['namespace'] = 'foo'
        ret = square.compute_patch(config, loc, srv)
        assert ret.data is None and ret.err is not None

        # Different namespace manifests (second one has labels).
        loc = make_manifest(kind, None, name)
        srv = copy.deepcopy(loc)
        loc['metadata']['labels'] = {"key": "value"}

        ret = square.compute_patch(config, loc, srv)
        assert ret.err is None and len(ret.data) > 0


class TestK8sConfig:
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
