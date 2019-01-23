import copy
import json
import os
import types
import unittest.mock as mock
import k8s_utils

import requests_mock

import pytest

import square

# Convenience.
pjoin = os.path.join
RetVal, DeploymentPlan = square.RetVal, square.DeploymentPlan
ManifestMeta = square.ManifestMeta
Patch = square.Patch
requests = k8s_utils.requests


@pytest.fixture
def m_requests(request):
    with requests_mock.Mocker() as m:
        yield m


def make_manifest(kind: str, name: str, namespace: str):
    # fixme: assert that first letter is captical letter.
    kind = kind.capitalize()

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

    @pytest.mark.xfail
    def test_load_manifest_files(self):
        """Recursively load all manifests in a given folder."""
        assert False

    @pytest.mark.xfail
    def test_load_manifest_files_duplicate(self):
        """Abort if the same resource is specified multiple times.

        It does not matter if the duplicates occur in the same or different files.

        """
        assert False

    @pytest.mark.xfail
    def test_prune_manifests(self):
        """Remove all manifests that refer to "improper" namespaces.

        A namespace is "proper" iff we found a manifest for it.

        """
        # No proper namespaces `demo` - no resources must survive.
        man = {
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'demo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'invalid')),
        }
        assert square.prune_manifests(man) == set()

        # One proper namespace `demo` - resources in `invalid` must be removed.
        man = {
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'demo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'demo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'invalid')),
        }
        assert square.prune_manifests(man) == {
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'demo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'demo')),
        }

        # Two namespaces with resources in each.
        man = {
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'bar')),
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'bar')),
        }
        assert square.prune_manifests(man) == {
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'bar')),
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'bar')),
        }

        # Only namespaces.
        man = {
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'bar')),
        }
        assert square.prune_manifests(man) == {
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'bar')),
        }

    @pytest.mark.xfail
    def test_filter_manifests(self):
        """Retain only the user specified namespaces"""
        # Two namespaces with resources in each.
        fun = square.filter_manifests
        man = {
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'bar')),
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'bar')),
        }
        assert fun(man, ['foo', 'bar']) == RetVal(True, None, man)
        assert fun(man, ['foo', 'bar', 'foo']) == RetVal(True, None, man)
        assert fun(man, ('foo', 'bar', 'foo')) == RetVal(True, None, man)

        # Ignore non-existing namespaces. This is sensible because we may
        # actually want to create them.
        assert fun(man, ('foo', 'bar', 'x', 'y')) == RetVal(True, None, man)

        assert fun(man, ['foo']) == RetVal(True, None, {
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'foo')),
        })
        assert fun(man, ['bar']) == RetVal(True, None, {
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'foo')),
        })
        assert fun(man, ['invalid']) == RetVal(True, None, set())

    @pytest.mark.xfail
    def test_compute_plan_add_remove(self):
        fun = square.compute_plan

        # No change because local and cluster manifests are identical.
        local_man = {
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'bar')),
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'bar')),
        }
        cluster_man = local_man
        ret = fun(local_man, cluster_man)
        assert ret == RetVal(True, None, set())

        # Remove namespace with one resource in it from cluster.
        local_man = {
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'bar')),
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'bar')),
        }
        cluster_man = {
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'bar')),
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'bar')),

        }
        plan = DeploymentPlan(
            add=set(),
            rem={
                square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'bar')),
                square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'bar')),
            },
            mod=set(),
        )
        ret = fun(local_man, cluster_man)
        assert ret == RetVal(True, None, plan)

        # Add namespace with one resource in it.
        local_man = {
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'bar')),
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'bar')),
        }
        cluster_man = {
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'foo')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'foo')),
        }
        plan = DeploymentPlan(
            add={
                square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'bar')),
                square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'bar')),
            },
            rem=set(),
            mod=set(),
        )
        ret = fun(local_man, cluster_man)
        assert ret == RetVal(True, None, plan)

    @pytest.mark.xfail
    def test_compute_plan_partial_namespace(self):
        """Must throw error if NS is deleted but not all of its resources."""
        # Local files mention a resource in a namespace for which we have no manifest.
        fun = square.compute_plan
        local_man = {
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'foo')),
        }
        cluster_man = set()
        ret = fun(local_man, cluster_man)
        assert ret == RetVal(False, 'Unknown namespace <foo>', None)

        # Local files delete a namespace but reference resources in it.
        fun = square.compute_plan
        local_man = {
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'bar')),
        }
        cluster_man = {
            square.Meta('1.yaml', 'iowa', self._manifest('namespace', 'bar')),
            square.Meta('1.yaml', 'iowa', self._manifest('deployment', 'api', 'bar')),
        }
        ret = fun(local_man, cluster_man)
        assert ret == RetVal(False, 'Namespace <bar> is not empty', None)

    @pytest.mark.xfail
    def test_compute_plan_modify(self):
        """Compute patches."""
        assert False


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
        assert ret == RetVal(None, "Manifest is missing attributes")

        # Namespace manifest must not have a `metadata.namespace` attribute.
        invalid_manifest = {
            'apiVersion': 'v1',
            'kind': 'Namespace',
            'metadata': {'name': 'foo', 'namespace': 'bar'},
            'spec': {'some': 'thing'},
        }
        ret = square.manifest_metaspec(invalid_manifest)
        assert ret == RetVal(
            None,
            "Namespace manifest must not have metadata.namespace attribute",
        )

        # The `kind` attribute must be all lower case with a capital first letter.
        invalid_manifest = {
            'apiVersion': 'v1',
            'kind': 'deployment',
            'metadata': {'name': 'foo', 'namespace': 'bar'},
            'spec': {'some': 'thing'},
        }
        ret = square.manifest_metaspec(invalid_manifest)
        assert ret == RetVal(
            None,
            "<kind> attribute must be capitalised",
        )

        # Same test again: the `kind` attribute must be all lower case with a
        # capital first letter.
        invalid_manifest = {
            'apiVersion': 'v1',
            'kind': 'dePLOYment',
            'metadata': {'name': 'foo', 'namespace': 'bar'},
            'spec': {'some': 'thing'},
        }
        ret = square.manifest_metaspec(invalid_manifest)
        assert ret == RetVal(
            None,
            "<kind> attribute must be capitalised",
        )

    def test_manifest_metaspec_automanifests(self):
        manifest = make_manifest('Deployment', f'name_0', f'ns_0')
        ret = square.manifest_metaspec(manifest)
        assert ret.err is None

        manifest = make_manifest('Namespace', f'name_0', None)
        ret = square.manifest_metaspec(manifest)
        assert ret.err is None

        manifest = make_manifest('Namespace', f'name_0', 'ns')
        ret = square.manifest_metaspec(manifest)
        assert ret == RetVal(
            None,
            "Namespace manifest must not have metadata.namespace attribute",
        )

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
            assert ret == RetVal(None, "Manifest is missing attributes")

        # Metadata must contain at least 'name' field.
        invalid = copy.deepcopy(valid_deployment_manifest)
        del invalid['metadata']['name']
        ret = square.manifest_metaspec(invalid)
        assert ret == RetVal(None, "Manifest metadata is missing attributes")


class TestFetchFromK8s:
    def test_list_parser_ok(self):
        """Convert eg a DeploymentList into a Python dict of Deployments."""
        # Demo manifests.
        manifests = [
            make_manifest('Deployment', f'name_{_}', f'ns_{_}')
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
        assert ret.err is None

        # Verify the Python dict.
        assert len(manifests) == 3
        assert ret.data == {
            ManifestMeta('v1', 'Deployment', 'ns_0', 'name_0'): manifests[0],
            ManifestMeta('v1', 'Deployment', 'ns_1', 'name_1'): manifests[1],
            ManifestMeta('v1', 'Deployment', 'ns_2', 'name_2'): manifests[2],
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
        assert ret == RetVal(data={}, err=None)

        # Missing `apiVersion`.
        src = {'kind': 'DeploymentList', 'items': []}
        ret = square.list_parser(src)
        assert ret == RetVal(data=None, err='Invalid K8s List resource')

        # Missing `kind`.
        src = {'apiVersion': 'v1', 'items': []}
        ret = square.list_parser(src)
        assert ret == RetVal(data=None, err='Invalid K8s List resource')

        # Missing `items`.
        src = {'apiVersion': 'v1', 'kind': 'DeploymentList'}
        ret = square.list_parser(src)
        assert ret == RetVal(data=None, err='Invalid K8s List resource')

        # All fields present but `kind` does not end in List (case sensitive).
        for invalid_kind in ('Deploymentlist', 'Deployment'):
            src = {'apiVersion': 'v1', 'kind': invalid_kind, 'items': []}
            ret = square.list_parser(src)
            assert ret == RetVal(data=None, err='Invalid K8s List resource')

    def test_k8s_get_request_ok(self):
        """Simulate a successful K8s response for GET request."""
        # Dummies for K8s API URL and `requests` session.
        url = 'http://examples.com/'
        sess = requests.Session()
        payload = {'foo': 'bar'}

        for ret_code in range(200, 210):
            with requests_mock.Mocker() as m_requests:
                m_requests.get(
                    url,
                    json=payload,
                    status_code=ret_code,
                )

                assert square.k8s_get_request(sess, url) == RetVal(payload, None)

                assert len(m_requests.request_history) == 1
                assert m_requests.request_history[0].method == 'GET'
                assert m_requests.request_history[0].url == url

    @mock.patch.object(square, 'list_parser')
    def test_k8s_get_request_err_code(self, m_parser, m_requests):
        """Simulate an unsuccessful K8s response for GET request."""
        # Dummies for K8s API URL and `requests` session.
        url = 'http://examples.com/'
        sess = requests.Session()

        m_requests.get(
            url,
            text=json.dumps("some error"),
            status_code=400,
        )

        ret = square.k8s_get_request(sess, url)
        assert not m_parser.called
        assert ret == RetVal(None, "K8s responded with error")

    @mock.patch.object(square, 'list_parser')
    def test_k8s_get_request_err_json(self, m_parser, m_requests):
        """Simulate a corrupt JSON response from K8s."""
        # Dummies for K8s API URL and `requests` session.
        url = 'http://examples.com/'
        sess = requests.Session()

        corrupt_json = "{this is not valid] json;"
        m_requests.get(
            url,
            text=corrupt_json,
            status_code=200,
        )

        ret = square.k8s_get_request(sess, url)
        assert not m_parser.called
        assert ret == RetVal(None, f"K8s returned corrupt JSON")

    @mock.patch.object(square, 'list_parser')
    def test_k8s_get_request_connection_err(self, m_parser, m_requests):
        """Simulate an unsuccessful K8s response for GET request."""
        # Dummies for K8s API URL and `requests` session.
        url = 'http://examples.com/'
        sess = requests.Session()

        m_requests.get(
            url,
            exc=requests.exceptions.ConnectionError,
        )

        ret = square.k8s_get_request(sess, url)
        assert not m_parser.called
        assert ret == RetVal(None, "Connection error")


class TestPatchK8s:
    def test_k8s_patch_ok(self, m_requests):
        """Simulate a successful K8s PATCH request."""
        url = 'https://example.com'
        patch = {"some": "json"}

        # Dummy `requests` session for the test calls.
        sess = requests.Session()

        def additionalMatcher(req):
            assert req.headers['Content-Type'] == 'application/json-patch+json'
            assert req.json() == patch
            return True

        m_requests.patch(url, status_code=200, additional_matcher=additionalMatcher)
        ret = square.k8s_patch(sess, url, patch)
        assert ret == RetVal(None, None)

        assert len(m_requests.request_history) == 1
        assert m_requests.request_history[0].method == 'PATCH'

    def test_k8s_patch_err(self, m_requests):
        """Simulate K8s PATCH request returning an error response."""
        url = 'https://example.com'
        patch = {"some": "json"}

        # Dummy `requests` session for the test calls.
        sess = requests.Session()

        m_requests.patch(url, status_code=400)
        ret = square.k8s_patch(sess, url, patch)
        assert ret == RetVal(None, "K8s operation failed")

    def test_k8s_patch_connection_err(self, m_requests):
        """Simulate a connection error during PATCH request."""
        url = 'https://example.com'
        patch = {"some": "json"}

        # Dummy `requests` session for the test calls.
        sess = requests.Session()

        m_requests.patch(url, exc=requests.exceptions.ConnectionError)
        ret = square.k8s_patch(sess, url, patch)
        assert ret == RetVal(None, "Connection error")

    def test_compute_patch_empty(self):
        """Basic test: compute patch between two identical resources."""
        config = types.SimpleNamespace(url='http://examples.com/')
        k8s_version = '1.9'
        kind, ns, name = 'Deployment', 'ns', 'foo'

        url = square.resource_url(config, k8s_version, kind, ns) + f'/{name}'

        src = dst = make_manifest(kind, name, ns)
        ret = square.compute_patch(config, k8s_version, src, dst)
        assert ret == RetVal(Patch(url, []), None)
        assert isinstance(ret.data, Patch)

    def test_compute_patch_incompatible(self):
        config = types.SimpleNamespace(url='http://examples.com/')
        k8s_version = '1.9'
        kind, ns, name = 'Deployment', 'ns', 'foo'

        src = make_manifest(kind, name, ns)

        # `apiVersion` must match.
        dst = copy.deepcopy(src)
        dst['apiVersion'] = 'mismatch'
        ret = square.compute_patch(config, k8s_version, src, dst)
        assert ret == RetVal(None, "Cannot compute JSON patch for incompatible manifests")

        # `kind` must match.
        dst = copy.deepcopy(src)
        dst['kind'] = 'Mismatch'
        ret = square.compute_patch(config, k8s_version, src, dst)
        assert ret == RetVal(None, "Cannot compute JSON patch for incompatible manifests")

        # `name` must match.
        dst = copy.deepcopy(src)
        dst['metadata']['name'] = 'mismatch'
        ret = square.compute_patch(config, k8s_version, src, dst)
        assert ret == RetVal(None, "Cannot compute JSON patch for incompatible manifests")

        # `namespace` must match.
        dst = copy.deepcopy(src)
        dst['metadata']['namespace'] = 'mismatch'
        ret = square.compute_patch(config, k8s_version, src, dst)
        assert ret == RetVal(None, "Cannot compute JSON patch for incompatible manifests")

    def test_compute_patch_namespace(self):
        """Corner cases that are specific to `Namespace` resources.

        Namespaces are special because, by definition, they must not contain a
        `Namespace` attribute.
        """
        config = types.SimpleNamespace(url='http://examples.com/')
        k8s_version = '1.9'
        kind, name = 'Namespace', 'foo'

        url = square.resource_url(config, k8s_version, kind, None) + f'/{name}'

        # Identical namespace manifests.
        src = make_manifest(kind, name, None)
        dst = copy.deepcopy(src)
        ret = square.compute_patch(config, k8s_version, src, dst)
        assert ret == RetVal((url, []), None)

        # Second manifest specifies a `metadata.namespace` attribute. This is
        # invalid and must result in an error.
        src = make_manifest(kind, name, None)
        dst = copy.deepcopy(src)
        dst['metadata']['namespace'] = 'foo'
        ret = square.compute_patch(config, k8s_version, src, dst)
        assert ret.data is None and ret.err is not None

        # Different namespace manifests (second one has labels).
        src = make_manifest(kind, name, None)
        dst = copy.deepcopy(src)
        dst['metadata']['labels'] = {"key": "value"}

        ret = square.compute_patch(config, k8s_version, src, dst)
        assert ret.err is None and len(ret.data) > 0
