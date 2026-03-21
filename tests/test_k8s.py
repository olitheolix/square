import json
import os
import random
import ssl
import types
import unittest.mock as mock
from pathlib import Path
from typing import Tuple

import httpx
import pytest
import yaml

import square.k8s as k8s
import square.square
from square.dtypes import (
    ConnectionParameters, K8sConfig, K8sResource, MetaManifest,
    SelKindGroupNames as SKGN,
)

from .test_helpers import kind_available


class TestK8sDeleteGetPatchPost:
    def test_create_httpx_client_ok(self, k8sconfig):
        """Verify the HttpX client is correctly setup."""
        conparam = ConnectionParameters(connect=2, read=3, write=4, pool=5)

        # Create basic Kubernetes configuration.
        cfg = k8sconfig._replace(token="")
        new_cfg, err = k8s.create_httpx_client(cfg, conparam)
        assert not err
        assert "authorization" not in new_cfg.client.headers
        assert new_cfg.headers == {}

        # Create token based Kubernetes configuration.
        cfg = k8sconfig._replace(token="token")
        new_cfg, err = k8s.create_httpx_client(cfg, conparam)
        assert not err
        assert isinstance(new_cfg.client, httpx.AsyncClient)
        assert new_cfg.client.headers["authorization"] == "Bearer token"
        assert new_cfg.headers == {"authorization": "Bearer token"}

        # Path to valid certificate specimen.
        fname_client_crt = Path("tests/support/client.crt")
        fname_client_key = Path("tests/support/client.key")

        ccert = (fname_client_crt, fname_client_key)
        cfg = k8sconfig._replace(token="token", cert=ccert)
        new_cfg, err = k8s.create_httpx_client(cfg, conparam)
        assert not err
        assert isinstance(new_cfg.client, httpx.AsyncClient)
        assert new_cfg.client.headers["authorization"] == "Bearer token"
        assert new_cfg.headers == {"authorization": "Bearer token"}

    def test_create_httpx_client_extra_headers_ok(self, k8sconfig):
        """Verify the HttpX client is correctly setup."""
        # Create basic Kubernetes configuration.
        cfg = k8sconfig._replace(token="")

        conparam = ConnectionParameters()
        new_cfg, err = k8s.create_httpx_client(cfg, conparam)
        assert not err
        assert "foo" not in new_cfg.client.headers

        conparam = ConnectionParameters(k8s_extra_headers={"foo": "bar"})
        new_cfg, err = k8s.create_httpx_client(cfg, conparam)
        assert not err
        assert new_cfg.client.headers["foo"] == "bar"

    def test_create_ssl_context(self):
        cadata = Path("tests/support/client.crt").read_text()

        # Create SSL context will all default safe guards in place.
        ssl_ctx = k8s.create_ssl_context(cadata, disable_x509_strict=False)
        assert ssl_ctx.verify_flags & ssl.VERIFY_X509_STRICT

        # Create SSL context with disabled x509 check.
        ssl_ctx = k8s.create_ssl_context(cadata, disable_x509_strict=True)
        assert not ssl_ctx.verify_flags & ssl.VERIFY_X509_STRICT

    def test_create_httpx_client_timeout(self, k8sconfig):
        """Verify that the function installs the correct timeouts."""
        cfg = k8sconfig._replace(token="")

        timeout = ConnectionParameters(connect=2, read=3, write=4, pool=5)
        new_cfg, err = k8s.create_httpx_client(cfg, timeout)
        assert not err
        assert new_cfg.client.timeout.connect == timeout.connect
        assert new_cfg.client.timeout.read == timeout.read
        assert new_cfg.client.timeout.write == timeout.write
        assert new_cfg.client.timeout.pool == timeout.pool

    def test_create_httpx_client_timeout_mocked(self, k8sconfig):
        """Use mocks to ascertain the connection parameters.

        This test is necessary because some parameters, eg the connection
        limits are inaccessible from the client instance. Therefore, we need
        to ensure the client was created with the correct parameters.

        """
        cfg = k8sconfig._replace(token="")

        # Construct an explicit `Timeout` instance.
        timeout = ConnectionParameters(
            connect=2, read=3, write=4, pool=5,
            max_keepalive_connections=None,
            max_connections=None,
            keepalive_expiry=5.0,
        )

        # Create the client.
        with mock.patch.object(k8s.httpx, "AsyncHTTPTransport") as m_trans:
            with mock.patch.object(k8s.httpx, "AsyncClient") as m_client:
                k8s.create_httpx_client(cfg, timeout)

        # Verify that the correct limits were used.
        assert m_client.call_args_list[0][1]["limits"] == httpx.Limits(
            max_keepalive_connections=None,
            max_connections=None,
            keepalive_expiry=5.0,
        )

        assert m_trans.call_args_list[0][1]["retries"] == 0
        assert m_trans.call_args_list[0][1]["http1"] is True
        assert m_trans.call_args_list[0][1]["http2"] is True

    def test_create_httpx_client_err(self, k8sconfig, tmp_path: Path):
        """Must gracefully abort when there are certificate problems."""
        timeout = ConnectionParameters(connect=2, read=3, write=4, pool=5)

        # Must gracefully abort when the certificate files do not exist.
        fname_client_crt = tmp_path / "does-not-exist.crt"
        fname_client_key = tmp_path / "does-not-exist.key"

        cert = (fname_client_crt, fname_client_key)
        cfg = k8sconfig._replace(cert=cert)
        assert k8s.create_httpx_client(cfg, timeout) == (cfg, True)

        # Must gracefully abort when the certificates are corrupt.
        fname_client_crt.write_text("not a valid certificate")
        fname_client_key.write_text("not a valid certificate")

        cert = (fname_client_crt, fname_client_key)
        cfg = k8sconfig._replace(cert=cert)
        assert k8s.create_httpx_client(cfg, timeout) == (cfg, True)

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    async def test_request_ok(self, method, k8sconfig, respx_mock):
        """Simulate a successful K8s response for GET request."""
        # Dummy values for the K8s API request.
        url = 'http://examples.com/'
        headers = {"some": "headers"}
        payload = {"some": "payload"}
        response = {"some": "response"}

        # Assign a random HTTP status code.
        status_code = random.randint(100, 510)
        m_http = respx_mock.request(method, url, headers=headers)
        m_http.return_value = httpx.Response(status_code, json=response)

        # Verify that the function makes the correct request and returns the
        # expected result and HTTP status code.
        ret = await k8s.request(k8sconfig, method, url, payload, headers)
        assert ret == (response, status_code, False)

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    async def test_request_err_json(self, method, k8sconfig, respx_mock):
        """Simulate a corrupt JSON response from K8s."""
        # Dummies for K8s API URL and `httpx` client.
        url = 'http://examples.com/'

        # Construct a response with a corrupt JSON string.
        corrupt_json = "{this is not valid] json;"
        m_http = respx_mock.request(method, url)
        m_http.return_value = httpx.Response(200, text=corrupt_json)

        # Test function must not return correct status code but also an error.
        ret = await k8s.request(k8sconfig, method, url, None, None)
        assert ret == ({}, 200, True)

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    async def test_request_connection_err(self, method, k8sconfig, respx_mock):
        """Simulate an unsuccessful K8s response for GET request."""
        # Dummies for K8s API URL and `httpx` client.
        url = 'http://examples.com/'
        m_http = respx_mock.request(method, url)
        expected: Tuple[dict, int, bool] = ({}, -1, True)
        args = k8sconfig, method, url, None, None

        # Simulate RequestError.
        m_http.mock(side_effect=k8s.httpx.RequestError(message="test error"))
        assert await k8s.request(*args) == expected

        # Simulate SSLError.
        m_http.mock(side_effect=ssl.SSLError())
        assert await k8s.request(*args) == expected

        # Simulate KeyError.
        m_http.mock(side_effect=KeyError())
        assert await k8s.request(*args) == expected

        # Simulate TimeoutError.
        m_http.mock(side_effect=TimeoutError())
        assert await k8s.request(*args) == expected

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    async def test_request_retries(self, k8sconfig, method):
        """Simulate error to validate retry logic."""
        # Dummies for K8s API URL and `httpx` client.
        url = 'http://localhost.foo.blah.'

        # Trick: we cannot mock any of the `Tenacity` callback functions
        # because they are imported before PyTest can patch them. Therefore, we
        # will mock `urlparse` as a proxy since it is not used anywhere else
        # and can tell us the number of invocations.
        with mock.patch.object(k8s, "urlparse") as m_proxy:
            # Test function must return an empty response and an error.
            ret = await k8s.request(k8sconfig, method, url, None, None)
            assert ret == ({}, -1, True)

            # Callback must have been called 7 times because Square
            # set `stop_after_attempt(8)`.
            assert m_proxy.call_count == 7

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    async def test_request_invalid(self, method, k8sconfig):
        """Simulate connection errors due to invalid URL schemes."""
        urls = [
            "localhost",        # missing schema like "http://"
            "httpinvalid://localhost",
            "http://localhost.foo.blah",
        ]

        # Test function must not return a response but indicate an error.
        for url in urls:
            ret = await k8s.request(k8sconfig, method, url, None, None)
            assert ret == ({}, -1, True)

    @mock.patch.object(k8s, "request")
    async def test_delete_get_patch_post_ok(self, m_req, k8sconfig):
        """Simulate successful DELETE, GET, PATCH, POST requests.

        This test is for the various wrappers around the `request`
        function, which is why we mock it so as to return an HTTP code that
        constitutes a successful transaction for the respective request.

        """
        # Dummy values.
        path = "path"
        payload = {"pay": "load"}
        response = "response"

        # K8s DELETE request was successful iff its return status is 200 or 202
        # (202 means deleting was scheduled but is still pending, usually
        # because of grace periods).
        for code in (200, 202):
            m_req.reset_mock()
            m_req.return_value = (response, code, False)
            assert await k8s.delete(k8sconfig, path, payload) == (response, False)
            m_req.assert_called_once_with(
                k8sconfig, "DELETE", path, payload, headers=None)

        # K8s GET request was successful iff its return status is 200.
        m_req.reset_mock()
        m_req.return_value = (response, 200, False)
        assert await k8s.get(k8sconfig, path) == (response, False)
        m_req.assert_called_once_with(k8sconfig, "GET", path, payload=None, headers=None)

        # K8s PATCH request was successful iff its return status is 200.
        m_req.reset_mock()
        m_req.return_value = (response, 200, False)
        assert await k8s.patch(k8sconfig, path, [payload]) == (response, False)
        patch_headers = {'Content-Type': 'application/json-patch+json'}
        m_req.assert_called_once_with(k8sconfig, "PATCH", path, [payload], patch_headers)

        # K8s POST request was successful iff its return status is 201.
        m_req.reset_mock()
        m_req.return_value = (response, 201, False)
        assert await k8s.post(k8sconfig, path, payload) == (response, False)
        m_req.assert_called_once_with(k8sconfig, "POST", path, payload, headers=None)

    @mock.patch.object(k8s, "request")
    async def test_delete_get_patch_post_err(self, m_req, k8sconfig):
        """Simulate unsuccessful DELETE, GET, PATCH, POST requests.

        This test is for the various wrappers around the `request`
        function, which is why we mock it so as to return an HTTP code that
        constitutes an unsuccessful transaction for the respective request.

        """
        # Dummy values.
        path = "path"
        payload = {"pay": "load"}
        response = "response"

        # K8s DELETE request was unsuccessful because its return status is not 200.
        m_req.reset_mock()
        m_req.return_value = (response, 400, False)
        assert await k8s.delete(k8sconfig, path, payload) == (response, True)
        m_req.assert_called_once_with(k8sconfig, "DELETE", path, payload, headers=None)

        # K8s GET request was unsuccessful because its returns status is not 200.
        m_req.reset_mock()
        m_req.return_value = (response, 400, False)
        assert await k8s.get(k8sconfig, path) == (response, True)
        m_req.assert_called_once_with(k8sconfig, "GET", path, payload=None, headers=None)

        # K8s PATCH request was unsuccessful because its returns status is not 200.
        m_req.reset_mock()
        m_req.return_value = (response, 400, False)
        assert await k8s.patch(k8sconfig, path, [payload]) == (response, True)
        patch_headers = {'Content-Type': 'application/json-patch+json'}
        m_req.assert_called_once_with(k8sconfig, "PATCH", path, [payload], patch_headers)

        # K8s POST request was unsuccessful because its returns status is not 201.
        m_req.reset_mock()
        m_req.return_value = (response, 400, False)
        assert await k8s.post(k8sconfig, path, payload) == (response, True)
        m_req.assert_called_once_with(k8sconfig, "POST", path, payload, headers=None)


class TestK8sVersion:
    @mock.patch.object(k8s, "get")
    async def test_version_auto_ok(self, m_get, k8sconfig):
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
        k8sconfig = k8sconfig._replace(client=mock.MagicMock())

        # Test function must contact the K8s API and return a `Config` tuple
        # with the correct version number.
        config2, err = await k8s.version(k8sconfig)
        assert err is False
        assert isinstance(config2, K8sConfig)
        assert config2.version == "1.10"

        # Test function must have called out to `get` to retrieve the
        # version. Here we ensure it called the correct URL.
        m_get.assert_called_once_with(k8sconfig, f"{k8sconfig.url}/version")
        assert not k8sconfig.client.called

        # The return `Config` tuple must be identical to the input except for
        # the version number because "k8s.version" will have overwritten it.
        assert k8sconfig._replace(version="None") == config2._replace(version="None")
        del config2, err

    @mock.patch.object(k8s, "get")
    async def test_version_auto_err(self, m_get, k8sconfig):
        """Simulate an error when fetching the K8s version."""
        # Create vanilla `K8sConfig` instance.
        k8sconfig = k8sconfig._replace(client=mock.MagicMock())

        # Simulate an error in `get`.
        m_get.return_value = (None, True)

        # Test function must abort gracefully.
        assert await k8s.version(k8sconfig) == (K8sConfig(), True)


class TestUrlPathBuilder:
    async def k8sconfig(self, integrationtest: bool, ref_config):
        """Return a valid `K8sConfig`.

        The returned `K8sConfig` model is either a valid dummy or a genuine
        configuration to access the integration test cluster.

        """
        # Use a fake or genuine K8s cluster.
        if not integrationtest:
            return ref_config

        if not kind_available():
            pytest.skip()
        kubeconfig = Path("/tmp/kubeconfig-kind.yaml")

        # Create a genuine `K8sConfig` for our integration test cluster.
        k8sconfig, err = await k8s.cluster_config(
            kubeconfig, None, ConnectionParameters()
        )
        assert not err and k8sconfig
        assert k8sconfig.client is not None
        return k8sconfig

    @pytest.mark.parametrize("integrationtest", [False, True])
    async def test_resource_v1(self, integrationtest, k8sconfig):
        """Function must query the correct version of the API endpoint.

        This test uses a Pod, which is part of the core API and should be
        available in all Kubernetes versions. Furthermore, v1 resources
        are somewhat special because they do not technically have a group.

        """
        # Fixtures.
        k8sconfig = await self.k8sconfig(integrationtest, k8sconfig)
        err_resp = (K8sResource("", "", "", False, "", tuple()), True)

        for gv in ["pod", "pod.v1"]:
            # A particular Pod in a particular namespace.
            meta = MetaManifest("v1", "Pod", namespace="ns", name="name")
            skgn = SKGN(value=f"{gv}/name", ns="ns")
            for data in [meta, skgn]:
                assert isinstance(data, (MetaManifest, SKGN))
                res, err = k8s.resource(k8sconfig, data)
                assert not err
                assert res == K8sResource(
                    apiVersion="v1", kind="Pod", name="pods", namespaced=True,
                    url=f"{k8sconfig.url}/api/v1/namespaces/ns/pods/name",
                    all_names=("po", "pod", "pods"),
                    preferred=True,
                )

            # All Pods in a particular namespace.
            meta = MetaManifest("v1", "Pod", namespace="ns", name="")
            skgn = SKGN(value=gv, ns="ns")
            for data in [meta, skgn]:
                assert isinstance(data, (MetaManifest, SKGN))
                res, err = k8s.resource(k8sconfig, data)
                assert not err
                assert res == K8sResource(
                    apiVersion="v1", kind="Pod", name="pods", namespaced=True,
                    url=f"{k8sconfig.url}/api/v1/namespaces/ns/pods",
                    all_names=("po", "pod", "pods"),
                    preferred=True,
                )

            # All Pods in all namespaces.
            meta = MetaManifest("v1", "Pod", namespace="", name="")
            skgn = SKGN(value=gv)
            for data in [meta, skgn]:
                assert isinstance(data, (MetaManifest, SKGN))
                res, err = k8s.resource(k8sconfig, data)
                assert not err
                assert res == K8sResource(
                    apiVersion="v1", kind="Pod", name="pods", namespaced=True,
                    url=f"{k8sconfig.url}/api/v1/pods",
                    all_names=("po", "pod", "pods"),
                    preferred=True,
                )

            # A particular Service in all namespaces -> Invalid.
            assert k8s.resource(k8sconfig, SKGN(value=f"{gv}/name")) == err_resp

    async def test_resource_hpa_from_metamanifest(self, k8sconfig):
        """Verify API version retrieval with a HorizontalPodAutoscaler.

        This resource is available as {v1, v2, v2beta1 and v2beta2}.

        NOTE: this test is tailored to Kubernetes v1.24.

        """
        config = await self.k8sconfig(False, k8sconfig)
        MM = MetaManifest
        err_resp = (K8sResource("", "", "", False, "", tuple()), True)

        # Tuples of API version that we ask for (if any), and what the final
        # K8sResource element will contain.
        api_versions = [
            # We expect to get the version we asked for.
            ("autoscaling/v1", "autoscaling/v1", False),
            ("autoscaling/v2beta1", "autoscaling/v2beta1", False),
            ("autoscaling/v2beta2", "autoscaling/v2beta2", False),

            # Function must automatically determine the latest version of the resource.
            ("", "autoscaling/v2", True),
        ]

        # Convenience.
        kind, k8s_name = "HorizontalPodAutoscaler", "horizontalpodautoscalers"

        for gv, expected, preferred in api_versions:
            # A particular HPA in a particular namespace.
            res, err = k8s.resource(config, MM(gv, kind, namespace="ns", name="name"))
            assert not err
            assert res == K8sResource(
                apiVersion=expected,
                kind=kind,
                name=k8s_name,
                namespaced=True,
                url=f"{config.url}/apis/{expected}/namespaces/ns/{k8s_name}/name",
                all_names=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa"),
                preferred=preferred,
            )

            # All HPAs in all namespaces.
            res, err = k8s.resource(config, MM(gv, kind, None, None))
            assert not err
            assert res == K8sResource(
                apiVersion=expected,
                kind=kind,
                name=k8s_name,
                namespaced=True,
                url=f"{config.url}/apis/{expected}/{k8s_name}",
                all_names=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa"),
                preferred=preferred,
            )

            # All HPAs in a particular namespace.
            res, err = k8s.resource(config, MM(gv, kind, "ns", ""))
            assert not err
            assert res == K8sResource(
                apiVersion=expected,
                kind=kind,
                name=k8s_name,
                namespaced=True,
                url=f"{config.url}/apis/{expected}/namespaces/ns/{k8s_name}",
                all_names=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa"),
                preferred=preferred,
            )

            # A particular HPA in all namespaces -> Invalid.
            assert k8s.resource(config, MM(gv, kind, None, "name")) == err_resp

    async def test_resource_hpa_from_skgn(self, k8sconfig):
        """Verify API version retrieval with a HorizontalPodAutoscaler.

        This resource is available as {v1, v2, v2beta1 and v2beta2}.

        NOTE: this test is tailored to Kubernetes v1.24.

        """
        config = await self.k8sconfig(False, k8sconfig)
        MM = MetaManifest
        err_resp = (K8sResource("", "", "", False, "", tuple()), True)

        # Convenience.
        kind, k8s_name = "HorizontalPodAutoscaler", "horizontalpodautoscalers"

        # A particular HPA in a particular namespace.
        expected = "autoscaling/v2"
        res, err = k8s.resource(config, SKGN(value="hpa.autoscaling/name", ns="ns"))
        assert not err
        assert res == K8sResource(
            apiVersion=expected,
            kind=kind,
            name=k8s_name,
            namespaced=True,
            url=f"{config.url}/apis/{expected}/namespaces/ns/{k8s_name}/name",
            all_names=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa"),
            preferred=True,
        )

        # All HPAs in all namespaces.
        res, err = k8s.resource(config, SKGN(value="hpa.autoscaling", ns=""))
        assert not err
        assert res == K8sResource(
            apiVersion=expected,
            kind=kind,
            name=k8s_name,
            namespaced=True,
            url=f"{config.url}/apis/{expected}/{k8s_name}",
            all_names=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa"),
            preferred=True,
        )

        # All HPAs in a particular namespace.
        res, err = k8s.resource(config, SKGN(value="hpa.autoscaling", ns="ns"))
        assert not err
        assert res == K8sResource(
            apiVersion=expected,
            kind=kind,
            name=k8s_name,
            namespaced=True,
            url=f"{config.url}/apis/{expected}/namespaces/ns/{k8s_name}",
            all_names=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa"),
            preferred=True,
        )

        # A particular HPA in all namespaces -> Invalid.
        assert k8s.resource(config, SKGN(value="hpa.autoscaling/name", ns="")) == err_resp

    async def test_resource_event_integration(self, k8sconfig):
        """Verify API version retrieval for `Event` resource.

        This resource is available as {v1, events.k8s.io/v1}.

        NOTE: this test is tailored to Kubernetes v1.25.

        """
        config = await self.k8sconfig(True, k8sconfig)
        err_resp = (K8sResource("", "", "", False, "", tuple()), True)

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
        kind, k8s_name = "Event", "events"

        for src, prefix, expected in api_versions:
            # A particular Event API version in a particular namespace.
            meta = MetaManifest(src, kind, "ns", "name")
            for data in [meta, meta.skgn()]:
                assert isinstance(data, (MetaManifest, SKGN))
                res, err = k8s.resource(config, data)
                assert not err
                assert res == K8sResource(
                    apiVersion=expected,
                    kind=kind,
                    name=k8s_name,
                    namespaced=True,
                    url=f"{config.url}/{prefix}/{expected}/namespaces/ns/{k8s_name}/name",
                    all_names=("ev", "event", "events"),
                    preferred=True,
                )

            # All Events APIs in all namespaces.
            meta = MetaManifest(src, kind, None, None)
            for data in [meta, meta.skgn()]:
                assert isinstance(data, (MetaManifest, SKGN))
                res, err = k8s.resource(config, data)
                assert not err
                assert res == K8sResource(
                    apiVersion=expected,
                    kind=kind,
                    name=k8s_name,
                    namespaced=True,
                    url=f"{config.url}/{prefix}/{expected}/{k8s_name}",
                    all_names=("ev", "event", "events"),
                    preferred=True,
                )

            # All Events in a particular namespace.
            meta = MetaManifest(src, kind, "ns", "")
            for data in [meta, meta.skgn()]:
                assert isinstance(data, (MetaManifest, SKGN))
                res, err = k8s.resource(config, data)
                assert not err
                assert res == K8sResource(
                    apiVersion=expected,
                    kind=kind,
                    name=k8s_name,
                    namespaced=True,
                    url=f"{config.url}/{prefix}/{expected}/namespaces/ns/{k8s_name}",
                    all_names=("ev", "event", "events"),
                    preferred=True,
                )

            # A particular Event in all namespaces -> Invalid.
            meta = MetaManifest(src, kind, None, "name")
            for data in [meta, meta.skgn()]:
                assert isinstance(data, (MetaManifest, SKGN))
                assert k8s.resource(config, data) == err_resp

    @pytest.mark.parametrize("integrationtest", [False, True])
    @pytest.mark.parametrize("ns_kind", ["ns", "namespace", "namespaces"])
    async def test_resource_namespace(self, ns_kind, integrationtest, k8sconfig):
        """Verify with a Namespace resource.

        This one is a special case because namespaces not themselves namespaced
        yet Square's MetaManifest may specify one. This is a special case where
        the `namespace` field refers to the actual name of the namespace. This
        is a necessary implementation detail to properly support the selectors.

        """
        # Fixtures.
        config = await self.k8sconfig(integrationtest, k8sconfig)

        for gv in ["", "v1"]:
            # Ask for all namespaces. This must silently ignore the "namespace"
            # since the name of a namespace is in `.metadata.name` instead of
            # in `.metadata.namespace` as for every other resource.
            meta_1 = MetaManifest(gv, "Namespace", None, None)
            meta_2 = MetaManifest(gv, "Namespace", "ns", None)
            kg = f"{ns_kind}.{gv}" if gv else ns_kind
            skgn_1 = SKGN(value=kg, ns="")
            skgn_2 = SKGN(value=kg, ns="ns")
            for data in [meta_1, meta_2, skgn_1, skgn_2]:
                assert isinstance(data, (MetaManifest, SKGN))
                res, err = k8s.resource(config, data)
                assert not err
                assert res == K8sResource(
                    apiVersion="v1",
                    kind="Namespace",
                    name="namespaces",
                    namespaced=False,
                    url=f"{config.url}/api/v1/namespaces",
                    all_names=('namespace', 'namespaces', 'ns'),
                    preferred=True,
                )

            # Ask for a particular. This must silently ignore the "namespace"
            # since the name of a namespace is in `.metadata.name` instead of
            # in `.metadata.namespace` as for every other resource.
            meta_1 = MetaManifest(gv, "Namespace", None, "name")
            meta_2 = MetaManifest(gv, "Namespace", "ns", "name")
            for data in [meta_1, meta_2, meta_1.skgn(), meta_2.skgn()]:
                assert isinstance(data, (MetaManifest, SKGN))
                res, err = k8s.resource(config, data)
                assert not err
                assert res == K8sResource(
                    apiVersion="v1",
                    kind="Namespace",
                    name="namespaces",
                    namespaced=False,
                    url=f"{config.url}/api/v1/namespaces/name",
                    all_names=('namespace', 'namespaces', 'ns'),
                    preferred=True,
                )

    @pytest.mark.parametrize("integrationtest", [False, True])
    async def test_resource_clusterrole(self, integrationtest, k8sconfig):
        """Verify with a ClusterRole resource.

        This is a basic test since ClusterRoles only exist as
        rbac.authorization.k8s.io/v1 anymore.

        """
        # Fixtures.
        config = await self.k8sconfig(integrationtest, k8sconfig)

        # Tuples of API version that we ask for (if any), and what the final
        # K8sResource element will contain.
        group = "rbac.authorization.k8s.io/v1"
        api_versions = [
            # We expect to get the version we asked for.
            group,

            # Function must automatically determine the latest version of the resource.
            "",
        ]

        # Ask for K8s resources.
        for gv in api_versions:
            # All ClusterRoles. Must silently ignore the namespace if one is
            # specified since ClusterRoles are not namespaced.
            meta_1 = MetaManifest(gv, "ClusterRole", None, None)
            meta_2 = MetaManifest(gv, "ClusterRole", "ns", None)
            for data in [meta_1, meta_2, meta_1.skgn(), meta_2.skgn()]:
                assert isinstance(data, (MetaManifest, SKGN))
                res, err = k8s.resource(config, data)
                assert not err
                assert res == K8sResource(
                    apiVersion=group,
                    kind="ClusterRole",
                    name="clusterroles",
                    namespaced=False,
                    url=f"{config.url}/apis/{group}/clusterroles",
                    all_names=("clusterrole", "clusterroles"),
                    preferred=True,
                )

            # A particular ClusterRoles. Must silently ignore the namespace if
            # one is specified since ClusterRoles are not namespaced.
            meta_1 = MetaManifest(gv, "ClusterRole", None, "name")
            meta_2 = MetaManifest(gv, "ClusterRole", "ns", "name")
            for data in [meta_1, meta_2, meta_1.skgn(), meta_2.skgn()]:
                assert isinstance(data, (MetaManifest, SKGN))
                res, err = k8s.resource(config, data)
                assert not err
                assert res == K8sResource(
                    apiVersion=group,
                    kind="ClusterRole",
                    name="clusterroles",
                    namespaced=False,
                    url=f"{config.url}/apis/{group}/clusterroles/name",
                    all_names=("clusterrole", "clusterroles"),
                    preferred=True,
                )

    def test_parse_api_group(self):
        # Load specimen.
        resp = yaml.safe_load(open("tests/support/api-v1.yaml").read())

        # Parse the API groups.
        group_urls, short = k8s.parse_api_group("version", "url", resp)
        assert short == {
            'binding': 'Binding', 'bindings': 'Binding',
            'configmap': 'ConfigMap', 'configmaps': 'ConfigMap', 'cm': 'ConfigMap',
            'namespace': 'Namespace', 'namespaces/status': 'Namespace'
        }

        assert group_urls == [
            K8sResource(
                apiVersion='version',
                kind='ConfigMap',
                name='configmaps',
                all_names=("cm", "configmap", "configmaps"),
                namespaced=True,
                url='url',
            )
        ]

    @pytest.mark.parametrize("integrationtest", [False, True])
    async def test_resource_err(self, integrationtest, k8sconfig: K8sConfig):
        """Test various error scenarios."""
        # Fixtures.
        config = await self.k8sconfig(integrationtest, k8sconfig)
        err_resp = (K8sResource("", "", "", False, "", tuple()), True)
        MM = MetaManifest

        # Sanity check: ask for a valid Deployment.
        _, err = k8s.resource(config, MM("apps/v1", "Deployment", "ns", "name"))
        assert not err

        # Ask for a StatefulSet on a bogus API endpoint.
        assert k8s.resource(config, MM("bogus", "Deployment", "ns", "name")) == err_resp

        # Ask for a bogus K8s kind.
        assert k8s.resource(config, MM("v1", "Bogus", "ns", "name")) == err_resp
        assert k8s.resource(config, MM("", "Bogus", "ns", "name")) == err_resp

    @mock.patch.object(k8s, "get")
    async def test_compile_api_endpoints(self, m_get, k8sconfig: K8sConfig):
        """Compile all endpoints from a pre-recorded set of API responses."""
        # Make all web requests fail.
        m_get.return_value = ({}, True)
        assert await k8s.compile_api_endpoints(k8sconfig) is True

        # Make the call to /apis pass, but fail all subsequent ones.
        m_get.side_effect = [({"groups": []}, False), ({}, True)]
        assert await k8s.compile_api_endpoints(k8sconfig) is True

        # Sample return value for `https://k8s.com/apis`
        fake_api = json.loads(open("tests/support/apis-v1-15-short.json").read())

        # Pretend to be the K8s API and return the requested data from our
        # recorded set of responses.
        def supply_fake_api(_, url):
            path = url.partition("/")[2]
            return fake_api[path], False
        m_get.side_effect = supply_fake_api

        k8sconfig.apis.clear()
        assert await k8s.compile_api_endpoints(k8sconfig) is False
        assert isinstance(k8sconfig.apis, dict) and len(k8sconfig.apis) > 0

        r_deploy_apps_v1 = K8sResource(
            apiVersion="apps/v1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url="/apis/apps/v1",
            all_names=("deploy", "deployment", "deployments"),
            preferred=True,
        )
        r_deploy_apps_v1beta = K8sResource(
            apiVersion="apps/v1beta1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url="/apis/apps/v1beta1",
            all_names=("deploy", "deployment", "deployments"),
            preferred=False,
        )
        r_deploy_fake_v1 = K8sResource(
            apiVersion="fake.k8s.io/v1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url="/apis/fake.k8s.io/v1",
            all_names=("configmap", "deployment", "deployments", "fakedeploy"),
            preferred=True,
        )
        r_cm_v1 = K8sResource(
            apiVersion="v1",
            kind="ConfigMap",
            name="configmaps",
            namespaced=True,
            url="/api/v1",
            all_names=("cm", "configmap", "configmaps"),
            preferred=True,
        )
        r_po_v1 = K8sResource(
            apiVersion="v1",
            kind="Pod",
            name="pods",
            namespaced=True,
            url="/api/v1",
            all_names=("po", "pod", "pods"),
            preferred=True,
        )

        assert k8sconfig.apis == {
            "deploy": [r_deploy_apps_v1, r_deploy_apps_v1beta],
            "deployment": [r_deploy_apps_v1, r_deploy_apps_v1beta, r_deploy_fake_v1],
            "deployments": [r_deploy_apps_v1, r_deploy_apps_v1beta, r_deploy_fake_v1],
            "deploy.apps": [r_deploy_apps_v1, r_deploy_apps_v1beta],
            "deployment.apps": [r_deploy_apps_v1, r_deploy_apps_v1beta],
            "deployments.apps": [r_deploy_apps_v1, r_deploy_apps_v1beta],
            "deploy.apps/v1": [r_deploy_apps_v1],
            "deployment.apps/v1": [r_deploy_apps_v1],
            "deployments.apps/v1": [r_deploy_apps_v1],
            "deploy.apps/v1beta1": [r_deploy_apps_v1beta],
            "deployment.apps/v1beta1": [r_deploy_apps_v1beta],
            "deployments.apps/v1beta1": [r_deploy_apps_v1beta],
            "deployment.fake.k8s.io": [r_deploy_fake_v1],
            "deployments.fake.k8s.io": [r_deploy_fake_v1],
            "deployment.fake.k8s.io/v1": [r_deploy_fake_v1],
            "deployments.fake.k8s.io/v1": [r_deploy_fake_v1],
            "fakedeploy": [r_deploy_fake_v1],
            "fakedeploy.fake.k8s.io": [r_deploy_fake_v1],
            "fakedeploy.fake.k8s.io/v1": [r_deploy_fake_v1],
            "configmap.fake.k8s.io": [r_deploy_fake_v1],
            "configmap.fake.k8s.io/v1": [r_deploy_fake_v1],
            "pod": [r_po_v1],
            "pods": [r_po_v1],
            "po": [r_po_v1],
            "pod.v1": [r_po_v1],
            "pods.v1": [r_po_v1],
            "po.v1": [r_po_v1],

            # NOTE: `configmap` technically exists in r_deploy_fake_v1 as well
            # but native resources are special and will never be shadowed. This
            # matches the kubectl behaviour where `kubectl get pods` will always
            # return pods even though a CRD may have defined `pods` as well.
            "configmap": [r_cm_v1],
            "configmaps": [r_cm_v1],
            "cm": [r_cm_v1],
            "configmap.v1": [r_cm_v1],
            "configmaps.v1": [r_cm_v1],
            "cm.v1": [r_cm_v1],
        }

    async def test_validate_apis(self):
        # Fixtures.
        r_deploy_p = K8sResource(
            apiVersion="apps/v1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url="/apis/apps/v1",
            all_names=("deploy", "deployment", "deployments"),
            preferred=True,
        )
        r_deploy_np = r_deploy_p._replace(preferred=False)

        # Valid cases: each API must contain at least one element.
        assert not k8s._validate_apis({})
        assert not k8s._validate_apis({"deploy": [r_deploy_p]})
        assert not k8s._validate_apis({"deploy": [r_deploy_p, r_deploy_np]})
        assert not k8s._validate_apis({"deploy": [r_deploy_p, r_deploy_p, r_deploy_np]})
        assert not k8s._validate_apis({"deploy.apps": [r_deploy_p]})
        assert not k8s._validate_apis({"deploy.apps/v1": [r_deploy_p]})

        # Various invalid cases.
        assert k8s._validate_apis({"deploy": []})

        # Invalid: the fully qualified kind, group and version must have
        # exactly one entry by definition.
        assert k8s._validate_apis({"deploy.apps/v1": [r_deploy_p, r_deploy_p]})

        # `foo` is not one of the resource aliases.
        assert k8s._validate_apis({"foo": [r_deploy_p]})

    @pytest.mark.parametrize("integrationtest", [False, True])
    async def test_compile_api_endpoints_integrated(self, integrationtest, k8sconfig):
        """Ask for all endpoints and perform some sanity checks.

        This test is about `compile_api_endpoints` but we only inspect the
        K8sConfig structure which must have been populated by
        `compile_api_endpoints` under the hood.

        """
        square.square.setup_logging(2)

        # This will call `compile_api_endpoints` internally to populate fields
        # in `k8sconfig`.
        config = await self.k8sconfig(integrationtest, k8sconfig)

        # Verify some standard resource types.
        assert config.apis["namespace"] == [K8sResource(
            apiVersion="v1",
            kind="Namespace",
            name="namespaces",
            namespaced=False,
            url=f"{config.url}/api/v1",
            all_names=("namespace", "namespaces", "ns"),
            preferred=True,
        )]
        assert config.apis["pod"] == [K8sResource(
            apiVersion="v1",
            kind="Pod",
            name="pods",
            namespaced=True,
            url=f"{config.url}/api/v1",
            all_names=("po", "pod", "pods"),
            preferred=True,
        )]
        assert config.apis["deployment.apps/v1"] == [K8sResource(
            apiVersion="apps/v1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
            all_names=("deploy", "deployment", "deployments"),
            preferred=True,
        )]
        assert config.apis["hpa.autoscaling/v2"] == [K8sResource(
            apiVersion="autoscaling/v2",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v2",
            all_names=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa"),
            preferred=True,
        )]

        # Verify our CRD.
        assert config.apis["democrd.mycrd.com/v1"] == [K8sResource(
            apiVersion="mycrd.com/v1",
            kind="DemoCRD",
            name="democrds",
            namespaced=True,
            url=f"{config.url}/apis/mycrd.com/v1",
            all_names=("democrd", "democrds"),
            preferred=True,
        )]

    def test_pick_api_kind_only(self):
        """Infer the correct resource just from the Kind without the group."""
        r_foo = K8sResource(
            apiVersion="api-a.com/v1",
            kind="Foo",
            name="foo",
            namespaced=True,
            url="",
            all_names=("foo", "ambiguous"),
            preferred=True,
        )
        r_bar = K8sResource(
            apiVersion="api-b.com/v1",
            kind="Bar",
            name="bar",
            namespaced=True,
            url="",
            all_names=("bar", "ambiguous"),
            preferred=True,
        )

        # -------------------- Error Cases --------------------

        apis = {"foo": [r_foo], "bar": [r_bar], "ambiguous": [r_foo, r_bar]}
        apiVersion = namespace = name = ""

        # Invalid: resource does not exist.
        meta = MetaManifest(apiVersion, "unknown", namespace, name)
        _, err = k8s.pick_api(meta, apis)
        assert err

        # Invalid: two APIs provide a resource of the same name.
        meta = MetaManifest(apiVersion, "ambiguous", namespace, name)
        _, err = k8s.pick_api(meta, {"ambiguous": [r_foo, r_bar]})
        assert err

        # Invalid: `foo` has multiple preferred resources.
        meta = MetaManifest(apiVersion, "foo", namespace, name)
        _, err = k8s.pick_api(meta, {"foo": [r_foo, r_foo]})
        assert err

        # Invalid: MetaManifest.
        invalid_apikind = [
            ("non-empty", "kind.group"),
            ("non-empty", "kind.group/v1"),
        ]
        for gv, kind in invalid_apikind:
            meta = MetaManifest(gv, kind, namespace, name)
            _, err = k8s.pick_api(meta, {"foo": [r_foo]})
            assert err

        # -------------------- Kind Only --------------------

        r_foo_v1a1 = r_foo._replace(preferred=False, apiVersion="api-a.com/v1alpha1")
        r_foo_v1a2 = r_foo._replace(preferred=False, apiVersion="api-a.com/v1alpha2")
        r_foo_v2a1 = r_foo._replace(preferred=False, apiVersion="api-a.com/v2alpha1")
        r_foo_v2a2 = r_foo._replace(preferred=False, apiVersion="api-a.com/v2alpha2")
        r_foo_v1b1 = r_foo._replace(preferred=False, apiVersion="api-a.com/v1beta1")
        r_foo_v1b2 = r_foo._replace(preferred=False, apiVersion="api-a.com/v1beta2")
        r_foo_v2b1 = r_foo._replace(preferred=False, apiVersion="api-a.com/v2beta1")
        r_foo_v2b2 = r_foo._replace(preferred=False, apiVersion="api-a.com/v2beta2")
        r_foo_v1 = r_foo._replace(preferred=False, apiVersion="api-a.com/v1")
        r_foo_v2 = r_foo._replace(preferred=False, apiVersion="api-a.com/v2")
        r_foo_spam = r_foo._replace(preferred=False, apiVersion="api-a.com/spam")
        r_foo_eggs = r_foo._replace(preferred=False, apiVersion="api-a.com/eggs")

        # Must pick the one and only option.
        apiVersion = namespace = name = ""
        meta = MetaManifest(apiVersion, "foo", namespace, name)
        apis = {"foo": [r_foo_v1a1]}
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_v1a1

        # Repeat: capitalisation of kind must not matter.
        apiVersion = namespace = name = ""
        apis = {"foo": [r_foo_v1a1]}
        got, err = k8s.pick_api(MetaManifest(apiVersion, "FOO", namespace, name), apis)
        assert not err and got is r_foo_v1a1

        # Must pick the highest alpha version.
        apis = {"foo": [r_foo_v1a1, r_foo_v1a2]}
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_v1a2

        # Must pick the highest alpha version.
        apis = {"foo": [r_foo_v1a1, r_foo_v1a2, r_foo_v2a1, r_foo_v2a2]}
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_v2a2

        # Must pick the highest beta version.
        apis = {"foo": [r_foo_v1a1, r_foo_v1b1, r_foo_v1b2, r_foo_v2b1, r_foo_v2b2]}
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_v2b2

        # Must pick the highest non-alpha/beta version.
        apis = {"foo": [r_foo_v1a1, r_foo_v2a2, r_foo_v1b1, r_foo_v2b2, r_foo_v1]}
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_v1

        # Must pick the highest non-alpha/beta version.
        apis = {"foo": [r_foo_v1a1, r_foo_v1b1, r_foo_v2b2, r_foo_v1, r_foo_v2]}
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_v2

        # Must pick the highest stable version even in the presence of
        # non-canonical versions.
        apis = {"foo": [r_foo_spam, r_foo_eggs, r_foo_v1, r_foo_v2]}
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_v2

        # Must pick the lexicographical last version if all versions are non-canonical.
        apis = {"foo": [r_foo_eggs]}
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_eggs

        # Must pick the lexicographical last version if all versions are non-canonical.
        apis = {"foo": [r_foo_spam, r_foo_eggs]}
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_spam

        # -------------------- Group & Kind (but no version) --------------------
        apis = {"foo.api-a.com": [r_foo_v1a2, r_foo_v2b1, r_foo_v1, r_foo_v2]}
        meta = MetaManifest("", "foo.api-a.com", namespace, name)
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_v2

        apis = {"foo.api-a.com": [r_foo_v1a2, r_foo_v2b1]}
        meta = MetaManifest("", "foo.api-a.com", namespace, name)
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_v2b1

        # -------------------- Full GVK --------------------

        apis = {"foo.api-a.com/v1": [r_foo_v1], "foo.api-a.com/v2": [r_foo_v2]}
        meta = MetaManifest("api-a.com/v1", "foo", namespace, name)
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_v1

        meta = MetaManifest("", "foo.api-a.com/v1", namespace, name)
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_v1

        meta = MetaManifest("api-a.com/v2", "foo", namespace, name)
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_v2

        meta = MetaManifest("", "foo.api-a.com/v2", namespace, name)
        got, err = k8s.pick_api(meta, apis)
        assert not err and got is r_foo_v2


class TestK8sKubeconfig:
    @mock.patch.object(k8s.os, "getenv")
    def test_incluster(self, m_getenv, tmp_path):
        """Create dummy certificate files and ensure the function loads them."""
        # Fake environment variable.
        m_getenv.return_value = "1.2.3.4"

        # Create dummy certificate files.
        cafile = tmp_path / "cert"
        tokenfile = tmp_path / "token"

        # Must fail because neither of the files exists.
        assert k8s.load_incluster_config(tokenfile, cafile) == (K8sConfig(), True)

        # Create the files with dummy content.
        cafile.write_text("cert")
        tokenfile.write_text("token")

        # Now that the files exist we must get the proper `Config` structure.
        ret, err = k8s.load_incluster_config(tokenfile, cafile)
        assert not err
        assert ret == K8sConfig(
            url='https://1.2.3.4',
            token="token",
            cadata="cert",
            cert=None,
            version="",
            name="",
        )

    @mock.patch.object(k8s, "load_auto_config")
    @mock.patch.object(k8s, "version")
    @mock.patch.object(k8s, "compile_api_endpoints")
    async def test_cluster_config_mock(self, m_compile_endpoints, m_version,
                                       m_load_auto, k8sconfig):
        """Mock all dependent calls and just verify the error handling."""
        kubeconfig = Path("kubeconfig")
        kubecontext = None

        # Pretend that all functions return without error.
        m_load_auto.return_value = (k8sconfig, False)
        m_version.return_value = (k8sconfig, False)
        m_compile_endpoints.return_value = False

        # Must return without error.
        ret = await k8s.cluster_config(kubeconfig, kubecontext, ConnectionParameters())
        assert ret == (k8sconfig, False)

        # Gracefully abort if any function returns an error.
        m_compile_endpoints.return_value = True
        ret = await k8s.cluster_config(kubeconfig, kubecontext, ConnectionParameters())
        assert ret == (K8sConfig(), True)

    def test_run_external_command(self):
        """Call valid and invalid external commands."""
        # A valid command must succeed without error.
        _, stderr, err = k8s.run_external_command(["python", "--version"], {})
        assert (stderr, err) == ("", False)

        # Gracefully abort if we pass invalid arguments to a program.
        stdout, stderr, err = k8s.run_external_command(["python", "--invalid"], {})
        assert stdout == "" and err is True and stderr != ""

        # Gracefully handle the case where the external program does not exist.
        stdout, _, err = k8s.run_external_command(["invalid-binary"], {})
        assert stdout == "" and err is True

        # Gracefully handle non-Unicode strings.
        with mock.patch.object(k8s.subprocess, "run") as m_run:
            m_run.return_value = types.SimpleNamespace(stdout=b"\x80", returncode=0)
            assert k8s.run_external_command(["ls"], {}) == ("", "", True)

    @mock.patch.object(k8s, "load_incluster_config")
    @mock.patch.object(k8s, "load_minikube_config")
    @mock.patch.object(k8s, "load_kind_config")
    @mock.patch.object(k8s, "load_authenticator_config")
    def test_load_auto_config(self, m_auth, m_kind, m_mini, m_incluster):
        """`load_auto_config` must pick the first successful configuration."""
        fun = k8s.load_auto_config

        m_incluster.return_value = (K8sConfig(), False)
        m_mini.return_value = (K8sConfig(), False)
        m_kind.return_value = (K8sConfig(), False)
        m_auth.return_value = (K8sConfig(), False)

        # Authenticator succeeds.
        kubeconf, context = Path("kubeconf"), "context"
        assert fun(kubeconf, context) == m_auth.return_value
        m_auth.assert_called_once_with(kubeconf, context)

        # Authenticator fails but Incluster succeeds.
        m_auth.return_value = (K8sConfig(), True)
        assert fun(kubeconf, context) == m_incluster.return_value
        m_incluster.assert_called_once_with()

        # Authenticator & Incluster fail but KinD succeeds.
        m_incluster.return_value = (K8sConfig(), True)
        assert fun(kubeconf, context) == m_kind.return_value
        m_kind.assert_called_once_with(kubeconf, context)

        # Authenticator & Incluster & KinD fail but Minikube succeeds.
        m_kind.return_value = (K8sConfig(), True)
        assert fun(kubeconf, context) == m_mini.return_value
        m_mini.assert_called_once_with(kubeconf, context)

        # All fail.
        m_mini.return_value = (K8sConfig(), True)
        assert fun(kubeconf, context) == (K8sConfig(), True)

    def test_load_minikube_config_ok(self):
        # Load the K8s configuration for "minikube" context.
        fname = Path("tests/support/kubeconf.yaml")
        cadata = Path("tests/support/client.crt").read_text()

        # Verify the expected output.
        ref = K8sConfig(
            url="https://192.168.0.177:8443",
            token="",
            cadata=cadata,
            cert=(Path("client.crt"), Path("client.key")),
            version="",
            name="minikube",
        )
        expected = (ref, False)
        assert k8s.load_minikube_config(fname, "minikube") == expected

        # Function must also accept `Path` instances.
        assert expected == k8s.load_minikube_config(Path(fname), None)

        # Minikube also happens to be the default context, so not supplying an
        # explicit context must return the same information.
        assert expected == k8s.load_minikube_config(fname, None)

    @pytest.mark.parametrize("with_token", [False, True])
    def test_load_kind_config_ok(self, with_token):
        token = "secret" if with_token else ""
        context = "kind-token" if with_token else "kind"

        # Load the K8s configuration for a Kind cluster.
        fname = Path("tests/support/kubeconf.yaml")

        ret, err = k8s.load_kind_config(fname, context)
        assert not err
        assert ret.url == "https://localhost:8443"
        assert ret.token == token
        assert ret.version == ""
        assert ret.name == "kind"

        # Function must also accept `Path` instances.
        ret, err = k8s.load_kind_config(Path(fname), context)
        assert not err
        assert ret.url == "https://localhost:8443"
        assert ret.token == token
        assert ret.version == ""
        assert ret.name == "kind"

        # Function must have create the credential files.
        assert ret.cadata is not None
        assert ret.cert is not None and len(ret.cert) == 2

    @pytest.mark.parametrize("kubetype", ["kind", "minkube"])
    def test_load_minkube_kind_config_invalid_context_err(self, kubetype, tmp_path):
        """Gracefully abort if we cannot parse Minkube & KinD credentials."""
        fun = k8s.load_kind_config if kubetype == "kind" else k8s.load_minikube_config

        # Valid Kubeconfig file but it has no "invalid" context.
        fname = Path("tests/support/kubeconf.yaml")
        assert fun(fname, "invalid") == (K8sConfig(), True)

        # Valid Kubeconfig file but not for KinD.
        fname = Path("tests/support/kubeconf.yaml")
        assert fun(fname, "gke") == (K8sConfig(), True)

        # Create a corrupt Kubeconfig file.
        fname = tmp_path / "kubeconfig"
        fname.write_text("")
        assert fun(fname, None) == (K8sConfig(), True)

        # Try to load a non-existing file.
        fname = tmp_path / "does-not-exist"
        assert fun(fname, None) == (K8sConfig(), True)

    @pytest.mark.parametrize("context", ["aks", "eks", "gke"])
    @mock.patch.object(k8s, "run_external_command")
    def test_load_authenticator_config_ok(self, m_run, context):
        """Compile K8s configuration based on external authenticator apps."""
        assert context in ("aks", "eks", "gke")
        if context == "aks":
            auth_app = "kubelogin"
        elif context == "eks":
            auth_app = "aws-iam-authenticator"
        else:
            auth_app = "gke-gcloud-auth-plugin"

        # Mock the call to run the external `aws-iam-authenticator` tool.
        token = yaml.dump({"status": {"token": "token"}})
        m_run.return_value = (token, "", False)

        # Load the K8s configuration for the current `context`.
        fname = Path("tests/support/kubeconf.yaml")
        ret, err = k8s.load_authenticator_config(fname, context)
        assert not err and isinstance(ret, K8sConfig)

        # Must have put the certificate into a temporary file for Httpx to find.
        assert ret.cadata is not None

        # Verify the returned Kubernetes configuration.
        assert ret == K8sConfig(
            url=f"https://{context}.com",
            token="token",
            cadata=ret.cadata,
            cert=None,
            version="",
            name=context,
        )

        # Verify that the correct external command was called, including
        # environment variables. The "expected_*" values are directly from
        # "support/kubeconf.yaml".
        if context == "aks":
            args = []
            env = {}
        elif context == "eks":
            args = ["token", "-i", "eks-cluster-name"]
            env = {"foo": "bar"}
        else:
            assert context == "gke"
            args, env = [], {}

        expected_cmd = [auth_app] + args
        expected_env = os.environ.copy() | env

        actual_cmd, actual_env = m_run.call_args[0][0], m_run.call_args[0][1]
        assert actual_cmd == expected_cmd
        assert actual_env == expected_env

    @mock.patch.object(k8s, "run_external_command")
    def test_load_authenticator_config_err(self, m_run):
        """Load K8s configuration from demo kubeconfig."""
        # Valid kubeconfig file.
        fname = Path("tests/support/kubeconf.yaml")
        err_resp = (K8sConfig(), True)

        # Pretend that the authenticator app returned a valid but useless YAML.
        m_run.side_effect = None
        m_run.return_value = (yaml.dump({}), "", False)
        assert k8s.load_authenticator_config(fname, "eks") == err_resp

        # Pretend that the authenticator app returned an invalid YAML.
        m_run.side_effect = None
        invalid_yaml = "invalid :: - yaml".encode("utf8")
        m_run.return_value = (invalid_yaml, "", False)
        assert k8s.load_authenticator_config(fname, "eks") == err_resp

        # Pretend that the authenticator app ran without error but returned an
        # empty string. This typically happens if eg the AWS config files do not
        # exist for the selected AWS profile.
        m_run.side_effect = None
        m_run.return_value = ("", "", False)
        assert k8s.load_authenticator_config(fname, "eks") == err_resp

        # Must fail because `eks` is not the default context in the demo kubeconf file.
        assert k8s.load_authenticator_config(fname, None) == (K8sConfig(), True)

        # Must fail because Minikube does not use an external app to create the token.
        assert k8s.load_authenticator_config(fname, "minikube") == (K8sConfig(), True)

    def test_load_authenticator_config_err_integration(self):
        """Use the `integration-test` context.

        This context will specify the `ls` command with invalid arguments as
        the external authenticator app. This allows us to run the test function
        without any mocks and validate the error handling.

        """
        # Valid kubeconfig file.
        fname = Path("tests/support/kubeconf.yaml")
        err_resp = (K8sConfig(), True)
        assert k8s.load_authenticator_config(fname, "integration-test") == err_resp

    @mock.patch.object(k8s, "run_external_command")
    def test_load_all_supported_kubeconfig_styles(self, m_run):
        """Verify that we can load every config style from our Kubeconfig specimen."""
        # Kubeconfig specimen.
        fname = Path("tests/support/kubeconf.yaml")

        # Mock the call to the external authenticator and make it return a token.
        token = yaml.dump({"status": {"token": "token"}})
        m_run.return_value = (token, "", False)

        assert k8s.load_minikube_config(fname, "minikube")[1] is False
        assert k8s.load_kind_config(fname, "kind")[1] is False
        assert k8s.load_authenticator_config(fname, "gke")[1] is False
        assert k8s.load_authenticator_config(fname, "eks")[1] is False

    def test_load_start_config_invalid_input(self):
        """All `load_*_config` functions must fail gracefully.

        This is a simple summary test to call all `load_*_functions` with
        invalid or corrupt Kubeconfig data.

        """
        # Convenience.
        P = Path("tests/support")

        # Expected failure response.
        resp = (K8sConfig(), True)

        # `load_minikube_config` must fail due to invalid Kubenconfig.
        assert k8s.load_minikube_config(P / "invalid.yaml", None) == resp
        assert k8s.load_minikube_config(P / "invalid.yaml", "invalid") == resp
        assert k8s.load_minikube_config(P / "kubeconf.yaml", "invalid") == resp
        assert k8s.load_minikube_config(P / "kubeconf_invalid.yaml", "minkube") == resp

        # `load_kind_config` must fail due to invalid Kubenconfig.
        assert k8s.load_kind_config(P / "invalid.yaml", None) == resp
        assert k8s.load_kind_config(P / "invalid.yaml", "invalid") == resp
        assert k8s.load_kind_config(P / "kubeconf.yaml", "invalid") == resp
        assert k8s.load_kind_config(P / "kubeconf_invalid.yaml", "kind") == resp

        # `load_authenticator_config` must fail due to invalid Kubenconfig.
        assert k8s.load_authenticator_config(P / "invalid.yaml", None) == resp
        assert k8s.load_authenticator_config(P / "invalid.yaml", "invalid") == resp
        assert k8s.load_authenticator_config(P / "kubeconf.yaml", "invalid") == resp
        assert k8s.load_authenticator_config(P / "kubeconf_invalid.yaml", "eks") == resp
