import os
import pathlib
import random
import types
import unittest.mock as mock

import pytest
import requests_mock
import square.k8s as k8s
import yaml
from square.dtypes import SUPPORTED_KINDS, SUPPORTED_VERSIONS, Config


@pytest.fixture
def m_requests(request):
    with requests_mock.Mocker() as m:
        yield m


class TestK8sDeleteGetPatchPost:
    def test_session(self):
        """Verify the `requests.Session` object is correctly setup."""
        # Basic.
        config = Config("", None, "ca_cert", client_cert=None, version=None, name="")
        sess = k8s.session(config)
        assert "authorization" not in sess.headers

        # With access token.
        config = Config("", "token", "ca_cert", client_cert=None, version=None, name="")
        sess = k8s.session(config)
        assert sess.headers["authorization"] == f"Bearer token"

        # With access token and client certificate.
        ccert = k8s.ClientCert(crt="foo", key="bar")
        config = Config("", "token", "ca_cert", client_cert=ccert, version=None, name="")
        sess = k8s.session(config)
        assert sess.headers["authorization"] == f"Bearer token"
        assert sess.cert == ("foo", "bar")

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
        assert ret == (response, status_code)

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
        assert ret == (None, True)

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
        assert ret == (None, True)

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

        # K8s DELETE request was successful iff its return status is 200 or 202
        # (202 means deleting was scheduled but is still pending, usually
        # because of grace periods).
        for code in (200, 202):
            m_req.reset_mock()
            m_req.return_value = (response, 202)
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
        assert k8s.patch(client, path, payload) == (response, False)
        patch_headers = {'Content-Type': 'application/json-patch+json'}
        m_req.assert_called_once_with(client, "PATCH", path, payload, patch_headers)

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
        payload = "payload"
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
        assert k8s.patch(client, path, payload) == (response, True)
        patch_headers = {'Content-Type': 'application/json-patch+json'}
        m_req.assert_called_once_with(client, "PATCH", path, payload, patch_headers)

        # K8s POST request was unsuccessful because its returns status is not 201.
        m_req.reset_mock()
        m_req.return_value = (response, 400)
        assert k8s.post(client, path, payload) == (response, True)
        m_req.assert_called_once_with(client, "POST", path, payload, headers=None)


class TestK8sVersion:
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
        m_get.return_value = (response, None)

        # Create vanilla `Config` instance.
        m_client = mock.MagicMock()
        config = Config("url", "token", "ca_cert", "client_cert", None, "")

        # Test function must contact the K8s API and return a `Config` tuple
        # with the correct version number.
        config2, err = k8s.version(config, client=m_client)
        assert err is False
        assert isinstance(config2, k8s.Config)
        assert config2.version == "1.10"

        # Test function must have called out to `get` to retrieve the
        # version. Here we ensure it called the correct URL.
        m_get.assert_called_once_with(m_client, f"{config.url}/version")
        assert not m_client.called

        # The return `Config` tuple must be identical to the input except for
        # the version number because "k8s.version" will have overwritten it.
        assert config._replace(version=None) == config2._replace(version=None)
        del config2, err

        # Repeat the test for a Google idiosyncracy which likes to report the
        # minor version as eg "11+".
        response["minor"] = "11+"
        m_get.return_value = (response, None)
        config, err = k8s.version(config, client=m_client)
        assert config.version == "1.11"

    @mock.patch.object(k8s, "get")
    def test_version_auto_err(self, m_get):
        """Simulate an error when fetching the K8s version."""

        # Create vanilla `Config` instance.
        m_client = mock.MagicMock()
        config = Config("url", "token", "ca_cert", "client_cert", None, "")

        # Simulate an error in `get`.
        m_get.return_value = (None, True)

        # Test function must abort gracefully.
        assert k8s.version(config, m_client) == (None, True)


class TestUrlPathBuilder:
    def test_supported_resources_versions(self):
        """Verify the global variables.

        Those variables specify the supported K8s versions and resource types.

        """
        assert SUPPORTED_VERSIONS == ("1.9", "1.10", "1.11", "1.13", "1.14")
        assert SUPPORTED_KINDS == (
            "Namespace", "ConfigMap", "Secret", "PersistentVolumeClaim",
            "ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding", "ServiceAccount",
            "Service", "Deployment", "DaemonSet", "StatefulSet",
            "HorizontalPodAutoscaler", "Ingress",
        )

    def test_urlpath_ok(self):
        """Must work for all supported K8s versions and resources."""
        for version in SUPPORTED_VERSIONS:
            cfg = Config("url", "token", "ca_cert", "client_cert", version, "")
            for kind in SUPPORTED_KINDS:
                for ns in (None, "foo-namespace"):
                    path, err = k8s.urlpath(cfg, kind, ns)

                # Verify.
                assert err is False
                assert isinstance(path, str)

    def test_urlpath_err(self):
        """Test various error scenarios."""
        # Valid version but invalid resource kind or invalid namespace spelling.
        for version in SUPPORTED_VERSIONS:
            cfg = Config("url", "token", "ca_cert", "client_cert", version, "")

            # Invalid resource kind.
            assert k8s.urlpath(cfg, "fooresource", "ns") == (None, True)

            # Namespace names must be all lower case (K8s imposes this)...
            assert k8s.urlpath(cfg, "Deployment", "namEspACe") == (None, True)

        # Invalid version.
        cfg = Config("url", "token", "ca_cert", "client_cert", "invalid", "")
        assert k8s.urlpath(cfg, "Deployment", "valid-ns") == (None, True)


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
        assert k8s.load_incluster_config(fname_token, fname_cert) is None

        # Create the files with dummy content.
        fname_cert.write_text("cert")
        fname_token.write_text("token")

        # Now that the files exist we must get the proper Config structure.
        ret = k8s.load_incluster_config(fname_token, fname_cert)
        assert ret == Config(
            url=f'https://1.2.3.4',
            token="token",
            ca_cert=str(fname_cert),
            client_cert=None,
            version=None,
            name="",
        )

    @mock.patch.object(k8s, "load_incluster_config")
    @mock.patch.object(k8s, "load_minikube_config")
    @mock.patch.object(k8s, "load_kind_config")
    @mock.patch.object(k8s, "load_eks_config")
    @mock.patch.object(k8s, "load_gke_config")
    def test_load_auto_config(self, m_gke, m_eks, m_kind, m_mini, m_incluster):
        """`load_auto_config` must pick the first successful configuration."""
        fun = k8s.load_auto_config

        # Incluster returns a non-zero value.
        kubeconf, context = "kubeconf", "context"
        assert fun(kubeconf, context) == m_incluster.return_value
        m_incluster.assert_called_once_with()

        # Incluster fails but Minikube does not.
        m_incluster.return_value = None
        assert fun(kubeconf, context) == m_mini.return_value
        m_mini.assert_called_once_with(kubeconf, context)

        # Incluster & Minikube fail but KIND succeeds.
        m_mini.return_value = None
        assert fun(kubeconf, context) == m_kind.return_value
        m_kind.assert_called_once_with(kubeconf, context)

        # Incluster & Minikube & KIND fail but EKS succeeds.
        m_kind.return_value = None
        assert fun(kubeconf, context) == m_eks.return_value
        m_eks.assert_called_once_with(kubeconf, context, False)

        # Incluster & Minikube & KIND & EKS fail but GKE succeeds.
        m_eks.return_value = None
        assert fun(kubeconf, context) == m_gke.return_value
        m_gke.assert_called_once_with(kubeconf, context, False)

        # All fail.
        m_gke.return_value = None
        assert fun(kubeconf, context) is None

    def test_load_minikube_config_ok(self):
        # Load the K8s configuration for "minikube" context.
        fname = "tests/support/kubeconf.yaml"
        ret = k8s.load_minikube_config(fname, "minikube")

        # Verify the expected output.
        assert ret == Config(
            url="https://192.168.0.177:8443",
            token=None,
            ca_cert="ca.crt",
            client_cert=k8s.ClientCert(crt="client.crt", key="client.key"),
            version=None,
            name="clustername-minikube",
        )

        # Function must also accept pathlib.Path instances.
        assert ret == k8s.load_minikube_config(pathlib.Path(fname), None)

        # Minikube also happens to be the default context, so not supplying an
        # explicit context must return the same information.
        assert ret == k8s.load_minikube_config(fname, None)

        # Try to load a GKE context - must fail.
        assert k8s.load_minikube_config(fname, "gke") is None

    def test_load_kind_config_ok(self):
        # Load the K8s configuration for a Kind cluster.
        fname = "tests/support/kubeconf.yaml"
        ret = k8s.load_kind_config(fname, "kind")

        # Verify the expected output.
        assert ret == Config(
            url="https://localhost:8443",
            token=None,
            ca_cert=pathlib.Path("/tmp/kind.ca"),
            client_cert=k8s.ClientCert(
                crt=pathlib.Path("/tmp/kind-client.crt"),
                key=pathlib.Path("/tmp/kind-client.key"),
            ),
            version=None,
            name="kind",
        )

        # Function must also accept pathlib.Path instances.
        assert ret == k8s.load_kind_config(pathlib.Path(fname), "kind")

        # Function must have create the credential files.
        assert ret == k8s.load_kind_config(fname, "kind")
        assert pathlib.Path("/tmp/kind.ca").exists()
        assert pathlib.Path("/tmp/kind-client.crt").exists()
        assert pathlib.Path("/tmp/kind-client.key").exists()

        # Try to load a GKE context - must fail.
        assert k8s.load_kind_config(fname, "gke") is None

    def test_load_kind_config_invalid_context_err(self, tmp_path):
        """Gracefully abort if we cannot parse Kubeconfig."""
        # Valid Kubeconfig file but it has no "invalid" context.
        fname = "tests/support/kubeconf.yaml"
        assert k8s.load_kind_config(fname, "invalid") is None

        # Create a corrupt Kubeconfig file.
        fname = tmp_path / "kubeconfig"
        fname.write_text("")
        assert k8s.load_kind_config(fname, None) is None

        # Try to load a non-existing file.
        fname = tmp_path / "does-not-exist"
        assert k8s.load_kind_config(fname, None) is None

    @mock.patch.object(k8s.google.auth, "default")
    def test_load_gke_config_ok(self, m_google):
        """Load GKE configuration from demo kubeconfig."""
        # Skip the Google authentication part.
        m_google.return_value = (m_google, "project_id")
        m_google.token = "google token"

        # Load the K8s configuration for "gke" context.
        fname = "tests/support/kubeconf.yaml"
        ret = k8s.load_gke_config(fname, "gke")
        assert isinstance(ret, Config)

        # The certificate will be in a temporary folder because the `requests`
        # library insists on reading it from a file. Here we load that file and
        # manually insert its value into the returned Config structure. This
        # will make the verification step below easier to read.
        ca_cert = open(ret.ca_cert, "r").read().strip()
        ret = ret._replace(ca_cert=ca_cert)

        # Verify the expected output.
        assert ret == Config(
            url="https://1.2.3.4",
            token="google token",
            ca_cert="ca.cert",
            client_cert=None,
            version=None,
            name="clustername-gke",
        )

        # GKE is not the default context in the demo kubeconf file, which means
        # this must fail.
        assert ret != k8s.load_gke_config(fname, None)

        # Try to load a Minikube context - must fail.
        assert k8s.load_gke_config(fname, "minikube") is None

    @mock.patch.object(k8s.subprocess, "run")
    def test_load_eks_config_ok(self, m_run):
        """Load EKS configuration from demo kubeconfig."""
        # Mock the call to run the `aws-iam-authenticator` tool
        token = yaml.dump({"status": {"token": "EKS token"}})
        m_run.return_value = types.SimpleNamespace(stdout=token.encode("utf8"))

        # Load the K8s configuration for "eks" context.
        fname = "tests/support/kubeconf.yaml"
        ret = k8s.load_eks_config(fname, "eks")
        assert isinstance(ret, Config)

        # The certificate will be in a temporary folder because the `Requests`
        # library insists on reading it from a file. Here we load that file and
        # manually insert its value into the returned Config structure. This
        # will make the verification step below easier to read.
        ca_cert = open(ret.ca_cert, "r").read().strip()
        ret = ret._replace(ca_cert=ca_cert)

        # Verify the expected output.
        assert ret == Config(
            url="https://5.6.7.8",
            token="EKS token",
            ca_cert="ca.cert",
            client_cert=None,
            version=None,
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
        assert ret != k8s.load_eks_config(fname, None)

        # Try to load a Minikube context - must fail.
        assert k8s.load_eks_config(fname, "minikube") is None

    @mock.patch.object(k8s.subprocess, "run")
    def test_load_eks_config_err(self, m_run):
        """Load EKS configuration from demo kubeconfig."""
        # Valid kubeconfig file.
        fname = "tests/support/kubeconf.yaml"

        # Pretend the `aws-iam-authenticator` binary does not exist.
        m_run.side_effect = FileNotFoundError
        assert k8s.load_eks_config(fname, "eks") is None

        # Pretend that `aws-iam-authenticator` returned a valid but useless YAML.
        m_run.side_effect = None
        m_run.return_value = types.SimpleNamespace(stdout=yaml.dump({}).encode("utf8"))
        assert k8s.load_eks_config(fname, "eks") is None

        # Pretend that `aws-iam-authenticator` returned an invalid YAML.
        m_run.side_effect = None

        invalid_yaml = "invalid :: - yaml".encode("utf8")
        m_run.return_value = types.SimpleNamespace(stdout=invalid_yaml)
        assert k8s.load_eks_config(fname, "eks") is None

    def test_wrong_conf(self):
        # Minikube
        fun = k8s.load_minikube_config
        assert fun("tests/support/invalid.yaml", None) is None
        assert fun("tests/support/invalid.yaml", "invalid") is None
        assert fun("tests/support/kubeconf.yaml", "invalid") is None
        assert fun("tests/support/kubeconf_invalid.yaml", "minkube") is None

        # GKE
        assert k8s.load_gke_config("tests/support/invalid.yaml", None) is None
        assert k8s.load_gke_config("tests/support/invalid.yaml", "invalid") is None
        assert k8s.load_gke_config("tests/support/kubeconf.yaml", "invalid") is None
        assert k8s.load_gke_config("tests/support/kubeconf_invalid.yaml", "gke") is None

        # EKS
        assert k8s.load_eks_config("tests/support/invalid.yaml", None) is None
        assert k8s.load_eks_config("tests/support/invalid.yaml", "invalid") is None
        assert k8s.load_eks_config("tests/support/kubeconf.yaml", "invalid") is None
        assert k8s.load_eks_config("tests/support/kubeconf_invalid.yaml", "eks") is None
