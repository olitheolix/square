import random
import types
import unittest.mock as mock

import k8s
import pytest
import requests_mock
import square
from k8s import RetVal


@pytest.fixture
def m_requests(request):
    with requests_mock.Mocker() as m:
        yield m


class TestK8sDeleteGetPatchPost:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    def test_request_ok(self, method, m_requests):
        """Simulate a successful K8s response for GET request."""
        # Dummy values for the K8s API request.
        url = 'http://examples.com/'
        client = k8s.requests.Session()
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
        ret = k8s.request(client, method, url, payload, headers)
        assert ret == RetVal(response, status_code)

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    def test_request_err_json(self, method, m_requests):
        """Simulate a corrupt JSON response from K8s."""
        # Dummies for K8s API URL and `requests` session.
        url = 'http://examples.com/'
        client = k8s.requests.Session()

        # Construct a response with a corrupt JSON string.
        corrupt_json = "{this is not valid] json;"
        m_requests.request(
            method,
            url,
            text=corrupt_json,
            status_code=200,
        )

        # Test function must not return a response but indicate an error.
        ret = k8s.request(client, method, url, None, None)
        assert ret == RetVal(None, True)

    @pytest.mark.parametrize("method", ("DELETE", "GET", "PATCH", "POST"))
    def test_request_connection_err(self, method, m_requests):
        """Simulate an unsuccessful K8s response for GET request."""
        # Dummies for K8s API URL and `requests` session.
        url = 'http://examples.com/'
        client = k8s.requests.Session()

        # Construct the ConnectionError exception with a fake request object.
        # The fake is necessary to ensure that the exception handler extracts
        # the correct pieces of information from it.
        req = types.SimpleNamespace(method=method, url=url)
        exc = k8s.requests.exceptions.ConnectionError(request=req)

        # Simulate a connection error during the request to K8s.
        m_requests.request(method, url, exc=exc)
        ret = k8s.request(client, method, url, None, None)
        assert ret == RetVal(None, True)

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
        payload = "payload"
        response = "response"

        # K8s DELETE request was successful iff its return status is 200.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 200)
        assert k8s.delete(client, path, payload) == RetVal(response, False)
        m_req.assert_called_once_with(client, "DELETE", path, payload, headers=None)

        # K8s GET request was successful iff its return status is 200.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 200)
        assert k8s.get(client, path) == RetVal(response, False)
        m_req.assert_called_once_with(client, "GET", path, payload=None, headers=None)

        # K8s PATCH request was successful iff its return status is 200.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 200)
        assert k8s.patch(client, path, payload) == RetVal(response, False)
        patch_headers = {'Content-Type': 'application/json-patch+json'}
        m_req.assert_called_once_with(client, "PATCH", path, payload, patch_headers)

        # K8s POST request was successful iff its return status is 201.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 201)
        assert k8s.post(client, path, payload) == RetVal(response, False)
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
        payload = "payload"
        response = "response"

        # K8s DELETE request was unsuccessful because its returns status is not 200.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 400)
        assert k8s.delete(client, path, payload) == RetVal(response, True)
        m_req.assert_called_once_with(client, "DELETE", path, payload, headers=None)

        # K8s GET request was unsuccessful because its returns status is not 200.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 400)
        assert k8s.get(client, path) == RetVal(response, True)
        m_req.assert_called_once_with(client, "GET", path, payload=None, headers=None)

        # K8s PATCH request was unsuccessful because its returns status is not 200.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 400)
        assert k8s.patch(client, path, payload) == RetVal(response, True)
        patch_headers = {'Content-Type': 'application/json-patch+json'}
        m_req.assert_called_once_with(client, "PATCH", path, payload, patch_headers)

        # K8s POST request was unsuccessful because its returns status is not 201.
        m_req.reset_mock()
        m_req.return_value = RetVal(response, 400)
        assert k8s.post(client, path, payload) == RetVal(response, True)
        m_req.assert_called_once_with(client, "POST", path, payload, headers=None)


class TestK8sConfig:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    @mock.patch.object(k8s, "get")
    def test_version_auto_ok(self, m_get):
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
        config = k8s.Config("url", "token", "ca_cert", "client_cert", None)

        # Test function must contact the K8s API and return a `Config` tuple
        # with the correct version number.
        config2, err = k8s.version(config, client=m_client)
        assert err is None
        assert isinstance(config2, k8s.Config)
        assert config2.version == "1.10"

        # Test function must have called out to `get` to retrieve the
        # version. Here we ensure it called the correct URL.
        m_get.assert_called_once_with(m_client, f"{config.url}/version")
        assert not m_client.called

        # The return `Config` tuple must be identical to the input except for
        # the version number.
        assert config._replace(version=None) == config2._replace(version=None)

    @mock.patch.object(k8s, "get")
    def test_version_auto_err(self, m_get):
        """Simulate an error when fetching the K8s version."""

        # Create vanilla `Config` instance.
        m_client = mock.MagicMock()
        config = k8s.Config("url", "token", "ca_cert", "client_cert", None)

        # Simulate an error in `get`.
        m_get.return_value = RetVal(None, True)

        # Test function must abort gracefully.
        assert k8s.version(config, m_client) == RetVal(None, True)
