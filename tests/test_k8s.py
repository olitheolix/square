import json
import os
import pathlib
import random
import sys
import time
import types
import unittest.mock as mock

import httpx
import pytest
import yaml

import square.k8s as k8s
import square.square
from square.dtypes import Filepath, K8sConfig, K8sResource, MetaManifest

from .test_helpers import kind_available


@pytest.fixture
def nosleep():
    with mock.patch.object(time, "sleep") as m_sleep:
        yield m_sleep


class TestK8sDeleteGetPatchPost:
    def test_session(self, k8sconfig):
        """Verify the `requests.Session` object is correctly setup."""
        # Basic.
        config = k8sconfig._replace(token="")
        print("*", config.ca_cert)
        sess = k8s.session(config)
        assert "authorization" not in sess.headers

        # With access token.
        config = k8sconfig._replace(token="token")
        sess = k8s.session(config)
        assert sess.headers["authorization"] == "Bearer token"
        return

        # With access token and client certificate.
        ccert = k8s.K8sClientCert(crt=Filepath("foo"), key=Filepath("bar"))
        config = k8sconfig._replace(token="token", client_cert=ccert)
        sess = k8s.session(config)
        assert sess.headers["authorization"] == "Bearer token"
        assert sess.cert == ("foo", "bar")

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    def test_request_ok(self, method, respx_mock):
        """Simulate a successful K8s response for GET request."""
        # Dummy values for the K8s API request.
        url = 'http://examples.com/'
        client = k8s.httpx.Client()
        headers = {"some": "headers"}
        payload = {"some": "payload"}
        response = {"some": "response"}

        # Assign a random HTTP status code.
        status_code = random.randint(100, 510)
        m_http = respx_mock.request(method, url, headers=headers)
        m_http.return_value = httpx.Response(status_code, json=response)

        # Verify that the function makes the correct request and returns the
        # expected result and HTTP status code.
        ret = k8s.request(client, method, url, payload, headers)
        assert ret == (response, status_code)

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    def test_request_err_json(self, method, respx_mock):
        """Simulate a corrupt JSON response from K8s."""
        # Dummies for K8s API URL and `httpx` client.
        url = 'http://examples.com/'
        client = k8s.httpx.Client()

        # Construct a response with a corrupt JSON string.
        corrupt_json = "{this is not valid] json;"
        m_http = respx_mock.request(method, url)
        m_http.return_value = httpx.Response(200, text=corrupt_json)

        # Test function must not return a response but indicate an error.
        ret = k8s.request(client, method, url, None, None)
        assert ret == ({}, True)

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    def test_request_connection_err(self, method, respx_mock, nosleep):
        """Simulate an unsuccessful K8s response for GET request."""
        # Dummies for K8s API URL and `httpx` client.
        url = 'http://examples.com/'
        client = k8s.httpx.Client()

        # Simulate a generic RequestError error during the request.
        m_http = respx_mock.request(method, url)
        m_http.mock(side_effect=k8s.httpx.RequestError(message="message"))
        ret = k8s.request(client, method, url, None, None)
        assert ret == ({}, True)

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    def test_request_retries(self, nosleep, method):
        """Simulate connection timeout to validate retry logic."""
        # Dummies for K8s API URL and `httpx` client.
        url = 'http://localhost:12345/'
        client = k8s.httpx.Client()

        # Test function must not return a response but indicate an error.
        ret = k8s.request(client, method, url, None, None)
        assert ret == ({}, True)

        # Windows is different and seems to have built in retry and/or timeout
        # limits - no idea. Mac and Linux behave as expected.
        if not sys.platform.startswith("win"):
            assert nosleep.call_count == 20

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    def test_request_invalid(self, method, nosleep):
        """Simulate connection errors due to invalid URL schemes."""
        # Dummies for K8s API URL and `httpx` client.
        client = k8s.httpx.Client()

        urls = [
            "localhost",        # missing schema like "http://"
            "httpinvalid://localhost",
            "http://localhost-does-not-exist",
        ]

        # Test function must not return a response but indicate an error.
        for url in urls:
            ret = k8s.request(client, method, url, None, None)
            assert ret == ({}, True)

    @mock.patch.object(k8s, "request")
    def test_delete_get_patch_post_ok(self, m_req):
        """Simulate successful DELETE, GET, PATCH, POST requests.

        This test is for the various wrappers around the `request`
        function, which is why we mock it so as to return an HTTP code that
        constitutes a successful transaction for the respective request.

        """
        # Dummy values.
        client = "client"
        path = "path"
        payload = {"pay": "load"}
        response = "response"

        # K8s DELETE request was successful iff its return status is 200 or 202
        # (202 means deleting was scheduled but is still pending, usually
        # because of grace periods).
        for code in (200, 202):
            m_req.reset_mock()
            m_req.return_value = (response, code)
            assert k8s.delete(client, path, payload) == (response, False)
            m_req.assert_called_once_with(client, "DELETE", path, payload, headers=None)

        # K8s GET request was successful iff its return status is 200.
        m_req.reset_mock()
        m_req.return_value = (response, 200)
        assert k8s.get(client, path) == (response, False)
        m_req.assert_called_once_with(client, "GET", path, payload=None, headers=None)

        # K8s PATCH request was successful iff its return status is 200.
        m_req.reset_mock()
        m_req.return_value = (response, 200)
        assert k8s.patch(client, path, [payload]) == (response, False)
        patch_headers = {'Content-Type': 'application/json-patch+json'}
        m_req.assert_called_once_with(client, "PATCH", path, [payload], patch_headers)

        # K8s POST request was successful iff its return status is 201.
        m_req.reset_mock()
        m_req.return_value = (response, 201)
        assert k8s.post(client, path, payload) == (response, False)
        m_req.assert_called_once_with(client, "POST", path, payload, headers=None)

    @mock.patch.object(k8s, "request")
    def test_delete_get_patch_post_err(self, m_req):
        """Simulate unsuccessful DELETE, GET, PATCH, POST requests.

        This test is for the various wrappers around the `request`
        function, which is why we mock it so as to return an HTTP code that
        constitutes an unsuccessful transaction for the respective request.

        """
        # Dummy values.
        client = "client"
        path = "path"
        payload = {"pay": "load"}
        response = "response"

        # K8s DELETE request was unsuccessful because its returns status is not 200.
        m_req.reset_mock()
        m_req.return_value = (response, 400)
        assert k8s.delete(client, path, payload) == (response, True)
        m_req.assert_called_once_with(client, "DELETE", path, payload, headers=None)

        # K8s GET request was unsuccessful because its returns status is not 200.
        m_req.reset_mock()
        m_req.return_value = (response, 400)
        assert k8s.get(client, path) == (response, True)
        m_req.assert_called_once_with(client, "GET", path, payload=None, headers=None)

        # K8s PATCH request was unsuccessful because its returns status is not 200.
        m_req.reset_mock()
        m_req.return_value = (response, 400)
        assert k8s.patch(client, path, [payload]) == (response, True)
        patch_headers = {'Content-Type': 'application/json-patch+json'}
        m_req.assert_called_once_with(client, "PATCH", path, [payload], patch_headers)

        # K8s POST request was unsuccessful because its returns status is not 201.
        m_req.reset_mock()
        m_req.return_value = (response, 400)
        assert k8s.post(client, path, payload) == (response, True)
        m_req.assert_called_once_with(client, "POST", path, payload, headers=None)


class TestK8sVersion:
    @mock.patch.object(k8s, "get")
    def test_version_auto_ok(self, m_get, k8sconfig):
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
        m_get.return_value = (response, None)

        # Create vanilla `Config` instance.
        m_client = mock.MagicMock()
        k8sconfig = k8sconfig._replace(client=m_client)

        # Test function must contact the K8s API and return a `Config` tuple
        # with the correct version number.
        config2, err = k8s.version(k8sconfig)
        assert err is False
        assert isinstance(config2, K8sConfig)
        assert config2.version == "1.10"

        # Test function must have called out to `get` to retrieve the
        # version. Here we ensure it called the correct URL.
        m_get.assert_called_once_with(m_client, f"{k8sconfig.url}/version")
        assert not m_client.called

        # The return `Config` tuple must be identical to the input except for
        # the version number because "k8s.version" will have overwritten it.
        assert k8sconfig._replace(version="None") == config2._replace(version="None")
        del config2, err

        # Repeat the test for a Google idiosyncracy which likes to report the
        # minor version as eg "11+".
        response["minor"] = "11+"
        m_get.return_value = (response, None)
        config, err = k8s.version(k8sconfig)
        assert config.version == "1.11"

    @mock.patch.object(k8s, "get")
    def test_version_auto_err(self, m_get, k8sconfig):
        """Simulate an error when fetching the K8s version."""
        # Create vanilla `K8sConfig` instance.
        k8sconfig = k8sconfig._replace(client=mock.MagicMock())

        # Simulate an error in `get`.
        m_get.return_value = (None, True)

        # Test function must abort gracefully.
        assert k8s.version(k8sconfig) == (K8sConfig(), True)


class TestUrlPathBuilder:
    def k8sconfig(self, integrationtest, ref_config):
        """Return a valid K8sConfig for a dummy or the real integration test cluster.

        The `config` is a reference K8s config. Return it if `integrationtest
        is False`, otherwise go to the real cluster. Skip the integration test
        if there is no real cluster available right now.

        """
        # Use a fake or genuine K8s cluster.
        if not integrationtest:
            return ref_config

        if not kind_available():
            pytest.skip()
        kubeconfig = Filepath("/tmp/kubeconfig-kind.yaml")

        # Create a genuine K8s config from our integration test cluster.
        k8sconfig, err = k8s.cluster_config(kubeconfig, None)
        assert not err and k8sconfig and k8sconfig.client
        return k8sconfig

    @pytest.mark.parametrize("integrationtest", [False, True])
    def test_resource_service(self, integrationtest, k8sconfig):
        """Function must query the correct version of the API endpoint.

        This test uses a Service which is available as part of the core API.

        NOTE: this test is tailored to Kubernetes v1.24.

        """
        # Fixtures.
        k8sconfig = self.k8sconfig(integrationtest, k8sconfig)
        err_resp = (K8sResource("", "", "", False, ""), True)

        # Tuples of API version that we ask for (if any), and what the final
        # K8sResource element will contain.
        api_versions = [
            # We expect to get the version we asked for.
            ("v1", "v1"),

            # Function must automatically determine the latest version of the resource.
            ("", "v1"),
        ]

        for src, expected in api_versions:
            # A particular Service in a particular namespace.
            res, err = k8s.resource(k8sconfig, MetaManifest(src, "Service", "ns", "name"))
            assert not err
            assert res == K8sResource(
                apiVersion=expected, kind="Service", name="services", namespaced=True,
                url=f"{k8sconfig.url}/api/v1/namespaces/ns/services/name",
            )

            # All Services in all namespaces.
            res, err = k8s.resource(k8sconfig, MetaManifest(src, "Service", None, None))
            assert not err
            assert res == K8sResource(
                apiVersion=expected, kind="Service", name="services", namespaced=True,
                url=f"{k8sconfig.url}/api/v1/services",
            )

            # All Services in a particular namespace.
            res, err = k8s.resource(k8sconfig, MetaManifest(src, "Service", "ns", ""))
            assert not err
            assert res == K8sResource(
                apiVersion=expected, kind="Service", name="services", namespaced=True,
                url=f"{k8sconfig.url}/api/v1/namespaces/ns/services",
            )

            # A particular Service in all namespaces -> Invalid.
            MM = MetaManifest
            assert k8s.resource(k8sconfig, MM(src, "Service", None, "name")) == err_resp

    def test_resource_hpa(self, k8sconfig):
        """Verify API version retrieval with a HorizontalPodAutoscaler.

        This resource is available as {v1, v2, v2beta1 and v2beta2}.

        NOTE: this test is tailored to Kubernetes v1.24.

        """
        config = self.k8sconfig(False, k8sconfig)
        MM = MetaManifest
        err_resp = (K8sResource("", "", "", False, ""), True)

        # Tuples of API version that we ask for (if any), and what the final
        # K8sResource element will contain.
        api_versions = [
            # We expect to get the version we asked for.
            ("autoscaling/v1", "autoscaling/v1"),
            ("autoscaling/v2beta1", "autoscaling/v2beta1"),
            ("autoscaling/v2beta2", "autoscaling/v2beta2"),

            # Function must automatically determine the latest version of the resource.
            ("", "autoscaling/v2"),
        ]

        # Convenience.
        kind = "HorizontalPodAutoscaler"
        name = kind.lower() + "s"

        for src, expected in api_versions:
            # A particular HPA in a particular namespace.
            res, err = k8s.resource(config, MM(src, kind, "ns", "name"))
            assert not err
            assert res == K8sResource(
                apiVersion=expected,
                kind=kind,
                name=name,
                namespaced=True,
                url=f"{config.url}/apis/{expected}/namespaces/ns/{name}/name",
            )

            # All HPAs in all namespaces.
            res, err = k8s.resource(config, MM(src, kind, None, None))
            assert not err
            assert res == K8sResource(
                apiVersion=expected,
                kind=kind,
                name=name,
                namespaced=True,
                url=f"{config.url}/apis/{expected}/{name}",
            )

            # All HPAs in a particular namespace.
            res, err = k8s.resource(config, MM(src, kind, "ns", ""))
            assert not err
            assert res == K8sResource(
                apiVersion=expected,
                kind=kind,
                name=name,
                namespaced=True,
                url=f"{config.url}/apis/{expected}/namespaces/ns/{name}",
            )

            # A particular HPA in all namespaces -> Invalid.
            assert k8s.resource(config, MM(src, kind, None, "name")) == err_resp

    def test_resource_event_integration(self, k8sconfig):
        """Verify API version retrieval for `Event` resource.

        This resource is available as {v1, events.k8s.io/v1}.

        NOTE: this test is tailored to Kubernetes v1.25.

        """
        config = self.k8sconfig(True, k8sconfig)
        MM = MetaManifest
        err_resp = (K8sResource("", "", "", False, ""), True)

        # Tuples of API version that we ask for (if any), and what the final
        # K8sResource element will contain.
        api_versions = [
            # We expect to get the version we asked for.
            ("v1", "api", "v1"),
            ("events.k8s.io/v1", "apis", "events.k8s.io/v1"),

            # Function must automatically determine the latest version of the resource.
            ("", "api", "v1"),
        ]

        # Convenience.
        kind = "Event"
        name = kind.lower() + "s"

        for src, prefix, expected in api_versions:
            # A particular Event API version in a particular namespace.
            res, err = k8s.resource(config, MM(src, kind, "ns", "name"))
            assert not err
            assert res == K8sResource(
                apiVersion=expected,
                kind=kind,
                name=name,
                namespaced=True,
                url=f"{config.url}/{prefix}/{expected}/namespaces/ns/{name}/name",
            )

            # All Events APIs in all namespaces.
            res, err = k8s.resource(config, MM(src, kind, None, None))
            assert not err
            assert res == K8sResource(
                apiVersion=expected,
                kind=kind,
                name=name,
                namespaced=True,
                url=f"{config.url}/{prefix}/{expected}/{name}",
            )

            # All Events in a particular namespace.
            res, err = k8s.resource(config, MM(src, kind, "ns", ""))
            assert not err
            assert res == K8sResource(
                apiVersion=expected,
                kind=kind,
                name=name,
                namespaced=True,
                url=f"{config.url}/{prefix}/{expected}/namespaces/ns/{name}",
            )

            # A particular Event in all namespaces -> Invalid.
            assert k8s.resource(config, MM(src, kind, None, "name")) == err_resp

    @pytest.mark.parametrize("integrationtest", [False, True])
    def test_resource_namespace(self, integrationtest, k8sconfig):
        """Verify with a Namespace resource.

        This one is a special case because namespaces not themselves namespaced
        yet Square's MetaManifest may specify one. This is a special case where
        the `namespace` field refers to the actual name of the namespace. This
        is a necessary implementation detail to properly support the selectors.

        """
        # Fixtures.
        config = self.k8sconfig(integrationtest, k8sconfig)
        MM = MetaManifest

        for src in ["", "v1"]:
            # A particular Namespace.
            res, err = k8s.resource(config, MM(src, "Namespace", None, "name"))
            assert not err
            assert res == K8sResource(
                apiVersion="v1",
                kind="Namespace",
                name="namespaces",
                namespaced=False,
                url=f"{config.url}/api/v1/namespaces/name",
            )

            # A particular Namespace in a particular namespace -> Invalid.
            assert k8s.resource(config, MM(src, "Namespace", "ns", "name")) == (res, err)

            # All Namespaces.
            res, err = k8s.resource(config, MM(src, "Namespace", None, None))
            assert not err
            assert res == K8sResource(
                apiVersion="v1",
                kind="Namespace",
                name="namespaces",
                namespaced=False,
                url=f"{config.url}/api/v1/namespaces",
            )

            # Same as above because the "namespace" argument is ignored for Namespaces.
            assert k8s.resource(config, MM(src, "Namespace", "name", "")) == (res, err)

    @pytest.mark.parametrize("integrationtest", [False, True])
    def test_resource_clusterrole(self, integrationtest, k8sconfig):
        """Verify with a ClusterRole resource.

        This is a basic test since ClusterRoles only exist as
        rbac.authorization.k8s.io/v1 anymore.

        """
        # Fixtures.
        config = self.k8sconfig(integrationtest, k8sconfig)
        MM = MetaManifest

        # Tuples of API version that we ask for (if any), and what the final
        # K8sResource element will contain.
        api_versions = [
            # We expect to get the version we asked for.
            ("rbac.authorization.k8s.io/v1", "rbac.authorization.k8s.io/v1"),

            # Function must automatically determine the latest version of the resource.
            ("", "rbac.authorization.k8s.io/v1"),
        ]

        # Ask for K8s resources.
        for src, expected in api_versions:
            # All ClusterRoles.
            res, err = k8s.resource(config, MM(src, "ClusterRole", None, None))
            assert not err
            assert res == K8sResource(
                apiVersion=expected,
                kind="ClusterRole",
                name="clusterroles",
                namespaced=False,
                url=f"{config.url}/apis/{expected}/clusterroles",
            )

            # All ClusterRoles in a particular namespace -> same as above
            # because the namespace is ignored for non-namespaced resources.
            assert k8s.resource(config, MM(src, "ClusterRole", "ns", None)) == (res, err)

            # A particular ClusterRole.
            res, err = k8s.resource(config, MM(src, "ClusterRole", None, "name"))
            assert not err
            assert res == K8sResource(
                apiVersion=expected,
                kind="ClusterRole",
                name="clusterroles",
                namespaced=False,
                url=f"{config.url}/apis/{expected}/clusterroles/name",
            )

            # A particular ClusterRole in a particular namespace -> Same as above
            # because the namespace is ignored for non-namespaced resources.
            assert k8s.resource(config, MM(src, "ClusterRole", "ns", "name")) == (res, err)  # noqa

    @pytest.mark.parametrize("integrationtest", [False, True])
    def test_resource_err(self, integrationtest, k8sconfig):
        """Test various error scenarios."""
        # Fixtures.
        config = self.k8sconfig(integrationtest, k8sconfig)
        err_resp = (K8sResource("", "", "", False, ""), True)
        MM = MetaManifest

        # Sanity check: ask for a valid StatefulSet.
        _, err = k8s.resource(config, MM("apps/v1", "StatefulSet", "ns", "name"))
        assert not err

        # Ask for a StatefulSet on a bogus API endpoint.
        assert k8s.resource(config, MM("bogus", "StatefulSet", "ns", "name")) == err_resp

        # Ask for a bogus K8s kind.
        assert k8s.resource(config, MM("v1", "Bogus", "ns", "name")) == err_resp
        assert k8s.resource(config, MM("", "Bogus", "ns", "name")) == err_resp

    @mock.patch.object(k8s, "get")
    def test_compile_api_endpoints(self, m_get, k8sconfig):
        """Compile all endpoints from a pre-recorded set of API responses."""
        # All web requests fail. Function must thus abort with an error.
        m_get.return_value = ({}, True)
        assert k8s.compile_api_endpoints(k8sconfig) is True

        # Sample return value for `https://k8s.com/apis`
        fake_api = json.loads(open("support/apis-v1-15.json").read())

        # Pretend to be the K8s API and return the requested data from our
        # recorded set of responses.
        def supply_fake_api(_, url):
            path = url.partition("/")[2]
            return fake_api[path], False
        m_get.side_effect = supply_fake_api

        k8sconfig.apis.clear()
        assert k8s.compile_api_endpoints(k8sconfig) is False
        assert isinstance(k8sconfig.apis, dict) and len(k8sconfig.apis) > 0

        # Services have a short name.
        assert k8sconfig.short2kind["service"] == "Service"
        assert k8sconfig.short2kind["services"] == "Service"
        assert k8sconfig.short2kind["svc"] == "Service"

        # Sanity check: must contain at least the default resource kinds. The
        # spelling must match with what would be declared in `manifest.kind`.
        assert "Service" in k8sconfig.kinds
        assert "service" not in k8sconfig.kinds
        assert "Services" not in k8sconfig.kinds
        assert "Deployment" in k8sconfig.kinds
        assert "deployment" not in k8sconfig.kinds
        assert "Deployments" not in k8sconfig.kinds

    @pytest.mark.parametrize("integrationtest", [False, True])
    def test_compile_api_endpoints_resource_kinds(self, integrationtest, k8sconfig):
        """Manually verify some resource kinds."""
        # Fixtures.
        config = self.k8sconfig(integrationtest, k8sconfig)

        # Sanity check: must contain at least the default resource kinds. The
        # spelling must match with what would be declared in `manifest.kind`.
        assert "Service" in config.kinds
        assert "service" not in config.kinds
        assert "Services" not in config.kinds
        assert "Deployment" in config.kinds
        assert "deployment" not in config.kinds
        assert "Deployments" not in config.kinds
        assert "CustomResourceDefinition" in config.kinds

        # Our demo CRD.
        assert "DemoCRD" in config.kinds

    @mock.patch.object(k8s, "get")
    def test_compile_api_endpoints_err(self, m_get, k8sconfig):
        """Simulate network errors while compiling API endpoints."""
        # All web requests fail. Function must thus abort with an error.
        m_get.return_value = ({}, True)
        assert k8s.compile_api_endpoints(k8sconfig) is True

        # Sample return value for `https://k8s.com/apis`
        ret = {
            "apiVersion": "v1",
            "groups": [{
                "name": "apps",
                "preferredVersion": {"groupVersion": "apps/v1", "version": "v1"},
                "versions": [
                    {"groupVersion": "apps/v1", "version": "v1"},
                    {"groupVersion": "apps/v1beta2", "version": "v1beta2"},
                    {"groupVersion": "apps/v1beta1", "version": "v1beta1"},
                ],
            }],
        }

        # Pretend that we could get all the API groups, but could not
        # interrogate the group endpoint to get the resources it offers.
        m_get.side_effect = [(ret, False), ({}, True)]
        assert k8s.compile_api_endpoints(k8sconfig) is True

    @pytest.mark.parametrize("integrationtest", [False, True])
    def test_compile_api_endpoints_integrated(self, integrationtest, k8sconfig):
        """Ask for all endpoints and perform some sanity checks.

        This test is about `compile_api_endpoints` but we only inspect the
        K8sConfig structure which must have been populated by
        `compile_api_endpoints` under the hood.

        """
        square.square.setup_logging(2)

        # This will call `compile_api_endpoints` internally to populate fields
        # in `k8sconfig`.
        config = self.k8sconfig(integrationtest, k8sconfig)

        # Sanity check.
        kinds = {
            # Some standard resources that every v1.24 Kubernetes cluster has.
            ('ClusterRole', 'rbac.authorization.k8s.io/v1'),
            ('ConfigMap', 'v1'),
            ('DaemonSet', 'apps/v1'),
            ('Deployment', 'apps/v1'),
            ('HorizontalPodAutoscaler', 'autoscaling/v2'),
            ('Pod', 'v1'),
            ('Service', 'v1'),
            ('ServiceAccount', 'v1'),

            # Our CustomCRD.
            ("DemoCRD", "mycrd.com/v1"),
        }
        assert kinds.issubset(set(config.apis.keys()))

        # Verify some standard resource types.
        assert config.apis[("Namespace", "v1")] == K8sResource(
            apiVersion="v1",
            kind="Namespace",
            name="namespaces",
            namespaced=False,
            url=f"{config.url}/api/v1",
        )
        hpa = "HorizontalPodAutoscaler"
        assert config.apis[(hpa, "autoscaling/v2")] == K8sResource(
            apiVersion="autoscaling/v2",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v2",
        )

        # The current integration test cluster does not have those endpoints
        # anymore, but for dry-run tests without a cluster they are still
        # defined in `test_helpers` and help to verify that Square
        # accurately lists all versions of the same resource.
        if not integrationtest:
            assert config.apis[(hpa, "autoscaling/v2beta1")] == K8sResource(
                apiVersion="autoscaling/v2beta1",
                kind="HorizontalPodAutoscaler",
                name="horizontalpodautoscalers",
                namespaced=True,
                url=f"{config.url}/apis/autoscaling/v2beta1",
            )
            assert config.apis[(hpa, "autoscaling/v2beta2")] == K8sResource(
                apiVersion="autoscaling/v2beta2",
                kind="HorizontalPodAutoscaler",
                name="horizontalpodautoscalers",
                namespaced=True,
                url=f"{config.url}/apis/autoscaling/v2beta2",
            )
        assert config.apis[("Pod", "v1")] == K8sResource(
            apiVersion="v1",
            kind="Pod",
            name="pods",
            namespaced=True,
            url=f"{config.url}/api/v1",
        )
        assert config.apis[("Deployment", "apps/v1")] == K8sResource(
            apiVersion="apps/v1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
        )
        assert config.apis[("Ingress", "networking.k8s.io/v1")] == K8sResource(
            apiVersion="networking.k8s.io/v1",
            kind="Ingress",
            name="ingresses",
            namespaced=True,
            url=f"{config.url}/apis/networking.k8s.io/v1",
        )

        # Verify our CRD.
        assert config.apis[("DemoCRD", "mycrd.com/v1")] == K8sResource(
            apiVersion="mycrd.com/v1",
            kind="DemoCRD",
            name="democrds",
            namespaced=True,
            url=f"{config.url}/apis/mycrd.com/v1",
        )

        # Verify default resource versions for a Deployment. In 1.24 the
        # default (and only) API version for Deployments is `apps/v1`.
        assert config.apis[("Deployment", "")].apiVersion == "apps/v1"


class TestK8sKubeconfig:
    @mock.patch.object(k8s.os, "getenv")
    def test_incluster(self, m_getenv, tmp_path):
        """Create dummy certificate files and ensure the function loads them."""
        # Fake environment variable.
        m_getenv.return_value = "1.2.3.4"

        # Create dummy certificate files.
        fname_cert = tmp_path / "cert"
        fname_token = tmp_path / "token"

        # Must fail because neither of the files exists.
        assert k8s.load_incluster_config(fname_token, fname_cert) == (K8sConfig(), True)

        # Create the files with dummy content.
        fname_cert.write_text("cert")
        fname_token.write_text("token")

        # Now that the files exist we must get the proper Config structure.
        ret, err = k8s.load_incluster_config(fname_token, fname_cert)
        assert not err
        assert ret == K8sConfig(
            url='https://1.2.3.4',
            token="token",
            ca_cert=fname_cert,
            client_cert=None,
            version="",
            name="",
        )

    @mock.patch.object(k8s, "load_auto_config")
    @mock.patch.object(k8s, "version")
    @mock.patch.object(k8s, "compile_api_endpoints")
    def test_cluster_config(self, m_compile_endpoints, m_version, m_load_auto, k8sconfig):
        kubeconfig = Filepath("kubeconfig")
        kubecontext = None

        m_load_auto.return_value = (k8sconfig, False)
        m_version.return_value = (k8sconfig, False)
        m_compile_endpoints.return_value = False

        assert k8s.cluster_config(kubeconfig, kubecontext) == (k8sconfig, False)

    @mock.patch.object(k8s, "load_incluster_config")
    @mock.patch.object(k8s, "load_minikube_config")
    @mock.patch.object(k8s, "load_kind_config")
    @mock.patch.object(k8s, "load_eks_config")
    @mock.patch.object(k8s, "load_gke_config")
    def test_load_auto_config(self, m_gke, m_eks, m_kind, m_mini, m_incluster):
        """`load_auto_config` must pick the first successful configuration."""
        fun = k8s.load_auto_config

        m_incluster.return_value = (K8sConfig(), False)
        m_mini.return_value = (K8sConfig(), False)
        m_kind.return_value = (K8sConfig(), False)
        m_eks.return_value = (K8sConfig(), False)
        m_gke.return_value = (K8sConfig(), False)

        # Incluster returns a non-zero value.
        kubeconf, context = Filepath("kubeconf"), "context"
        assert fun(kubeconf, context) == m_incluster.return_value
        m_incluster.assert_called_once_with()

        # Incluster fails but Minikube does not.
        m_incluster.return_value = (K8sConfig(), True)
        assert fun(kubeconf, context) == m_mini.return_value
        m_mini.assert_called_once_with(kubeconf, context)

        # Incluster & Minikube fail but KIND succeeds.
        m_mini.return_value = (K8sConfig(), True)
        assert fun(kubeconf, context) == m_kind.return_value
        m_kind.assert_called_once_with(kubeconf, context)

        # Incluster & Minikube & KIND fail but EKS succeeds.
        m_kind.return_value = (K8sConfig(), True)
        assert fun(kubeconf, context) == m_eks.return_value
        m_eks.assert_called_once_with(kubeconf, context, False)

        # Incluster & Minikube & KIND & EKS fail but GKE succeeds.
        m_eks.return_value = (K8sConfig(), True)
        assert fun(kubeconf, context) == m_gke.return_value
        m_gke.assert_called_once_with(kubeconf, context, False)

        # All fail.
        m_gke.return_value = (K8sConfig(), True)
        assert fun(kubeconf, context) == (K8sConfig(), True)

    def test_load_minikube_config_ok(self):
        # Load the K8s configuration for "minikube" context.
        fname = Filepath("tests/support/kubeconf.yaml")

        # Verify the expected output.
        ref = K8sConfig(
            url="https://192.168.0.177:8443",
            token="",
            ca_cert=Filepath("ca.crt"),
            client_cert=k8s.K8sClientCert(
                crt=Filepath("client.crt"),
                key=Filepath("client.key"),
            ),
            version="",
            name="clustername-minikube",
        )
        expected = (ref, False)
        assert k8s.load_minikube_config(fname, "minikube") == expected

        # Function must also accept pathlib.Path instances.
        assert expected == k8s.load_minikube_config(pathlib.Path(fname), None)

        # Minikube also happens to be the default context, so not supplying an
        # explicit context must return the same information.
        assert expected == k8s.load_minikube_config(fname, None)

        # Try to load a GKE context - must fail.
        assert k8s.load_minikube_config(fname, "gke") == (K8sConfig(), True)

    def test_load_kind_config_ok(self):
        # Load the K8s configuration for a Kind cluster.
        fname = Filepath("tests/support/kubeconf.yaml")

        ret, err = k8s.load_kind_config(fname, "kind")
        assert not err
        assert ret.url == "https://localhost:8443"
        assert ret.token == ""
        assert ret.version == ""
        assert ret.name == "kind"

        # Function must also accept pathlib.Path instances.
        ret, err = k8s.load_kind_config(pathlib.Path(fname), "kind")
        assert not err
        assert ret.url == "https://localhost:8443"
        assert ret.token == ""
        assert ret.version == ""
        assert ret.name == "kind"

        # Function must have create the credential files.
        assert ret.ca_cert.exists()
        assert ret.client_cert is not None
        assert ret.client_cert.crt.exists()
        assert ret.client_cert.key.exists()

        # Try to load a GKE context - must fail.
        assert k8s.load_kind_config(fname, "gke") == (K8sConfig(), True)

    def test_load_kind_config_invalid_context_err(self, tmp_path):
        """Gracefully abort if we cannot parse Kubeconfig."""
        # Valid Kubeconfig file but it has no "invalid" context.
        fname = Filepath("tests/support/kubeconf.yaml")
        assert k8s.load_kind_config(fname, "invalid") == (K8sConfig(), True)

        # Create a corrupt Kubeconfig file.
        fname = tmp_path / "kubeconfig"
        fname.write_text("")
        assert k8s.load_kind_config(fname, None) == (K8sConfig(), True)

        # Try to load a non-existing file.
        fname = tmp_path / "does-not-exist"
        assert k8s.load_kind_config(fname, None) == (K8sConfig(), True)

    @mock.patch.object(k8s.google.auth, "default")
    def test_load_gke_config_ok(self, m_google):
        """Load GKE configuration from demo kubeconfig."""
        # Skip the Google authentication part.
        m_google.return_value = (m_google, "project_id")
        m_google.token = "google token"

        # Load the K8s configuration for "gke" context.
        fname = Filepath("tests/support/kubeconf.yaml")
        ret, err = k8s.load_gke_config(fname, "gke")
        assert not err and isinstance(ret, K8sConfig)

        # The certificate will be in a temporary folder because the `Requests`
        # library insists on reading it from a file. Here we verify that the
        # test function did indeed create such a temporary file.
        assert ret.ca_cert.exists()

        # Verify the expected output.
        assert ret == K8sConfig(
            url="https://1.2.3.4",
            token="google token",
            ca_cert=ret.ca_cert,
            client_cert=None,
            version="",
            name="clustername-gke",
        )

        # GKE is not the default context in the demo kubeconf file, which means
        # this must fail.
        assert k8s.load_gke_config(fname, None) == (K8sConfig(), True)

        # Try to load a Minikube context - must fail.
        assert k8s.load_gke_config(fname, "minikube") == (K8sConfig(), True)

        # Pretend the Google client threw an exception.
        m_google.side_effect = k8s.google.auth.exceptions.DefaultCredentialsError
        assert k8s.load_gke_config(fname, "gke") == (K8sConfig(), True)

    @mock.patch.object(k8s.subprocess, "run")
    def test_load_eks_config_ok(self, m_run):
        """Load EKS configuration from demo kubeconfig."""
        # Mock the call to run the `aws-iam-authenticator` tool
        token = yaml.dump({"status": {"token": "EKS token"}})
        m_run.return_value = types.SimpleNamespace(stdout=token.encode("utf8"))

        # Load the K8s configuration for "eks" context.
        fname = Filepath("tests/support/kubeconf.yaml")
        ret, err = k8s.load_eks_config(fname, "eks")
        assert not err and isinstance(ret, K8sConfig)

        # The certificate will be in a temporary folder because the `Requests`
        # library insists on reading it from a file. Here we verify that the
        # test function did indeed create such a temporary file.
        assert ret.ca_cert.exists()

        # Verify the expected output.
        assert ret == K8sConfig(
            url="https://5.6.7.8",
            token="EKS token",
            ca_cert=ret.ca_cert,
            client_cert=None,
            version="",
            name="clustername-eks",
        )

        # Verify that the correct external command was called, including
        # environment variables. The "expected_*" values are directly from
        # "support/kubeconf.yaml".
        expected_cmd = ["aws-iam-authenticator", "token", "-i", "eks-cluster-name"]
        expected_env = os.environ.copy()
        expected_env.update({"foo1": "bar1", "foo2": "bar2"})
        actual_cmd, actual_env = m_run.call_args[0][0], m_run.call_args[1]["env"]
        assert actual_cmd == expected_cmd
        assert actual_env == expected_env

        # EKS is not the default context in the demo kubeconf file, which means
        # this must fail.
        assert k8s.load_eks_config(fname, None) == (K8sConfig(), True)

        # Try to load a Minikube context - must fail.
        assert k8s.load_eks_config(fname, "minikube") == (K8sConfig(), True)

    @mock.patch.object(k8s.subprocess, "run")
    def test_load_eks_config_err(self, m_run):
        """Load EKS configuration from demo kubeconfig."""
        # Valid kubeconfig file.
        fname = Filepath("tests/support/kubeconf.yaml")
        err_resp = (K8sConfig(), True)

        # Pretend the `aws-iam-authenticator` binary does not exist.
        m_run.side_effect = FileNotFoundError
        assert k8s.load_eks_config(fname, "eks") == err_resp

        # Pretend that `aws-iam-authenticator` returned a valid but useless YAML.
        m_run.side_effect = None
        m_run.return_value = types.SimpleNamespace(stdout=yaml.dump({}).encode("utf8"))
        assert k8s.load_eks_config(fname, "eks") == err_resp

        # Pretend that `aws-iam-authenticator` returned an invalid YAML.
        m_run.side_effect = None
        invalid_yaml = "invalid :: - yaml".encode("utf8")
        m_run.return_value = types.SimpleNamespace(stdout=invalid_yaml)
        assert k8s.load_eks_config(fname, "eks") == err_resp

        # Pretend that `aws-iam-authenticator` ran without error but
        # returned an empty string. This typically happens if the AWS config
        # files do not exist for the selected AWS profile.
        m_run.side_effect = None
        m_run.return_value = types.SimpleNamespace(stdout=b"")
        assert k8s.load_eks_config(fname, "eks") == err_resp

    def test_wrong_conf(self):
        # Minikube
        fun = k8s.load_minikube_config
        resp = (K8sConfig(), True)
        P = Filepath("tests/support")
        assert fun(P / "invalid.yaml", None) == resp
        assert fun(P / "invalid.yaml", "invalid") == resp
        assert fun(P / "kubeconf.yaml", "invalid") == resp
        assert fun(P / "kubeconf_invalid.yaml", "minkube") == resp

        # GKE
        assert k8s.load_gke_config(P / "invalid.yaml", None) == resp
        assert k8s.load_gke_config(P / "invalid.yaml", "invalid") == resp
        assert k8s.load_gke_config(P / "kubeconf.yaml", "invalid") == resp
        assert k8s.load_gke_config(P / "kubeconf_invalid.yaml",
                                   "gke") == resp

        # EKS
        assert k8s.load_eks_config(P / "invalid.yaml", None) == resp
        assert k8s.load_eks_config(P / "invalid.yaml", "invalid") == resp
        assert k8s.load_eks_config(P / "kubeconf.yaml", "invalid") == resp
        assert k8s.load_eks_config(P / "kubeconf_invalid.yaml", "eks") == resp
