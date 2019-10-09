import os
import pathlib
import types
import unittest.mock as mock

import pytest
import square.main as main
import square.square as square
from square.dtypes import (
    SUPPORTED_KINDS, Configuration, K8sConfig, ManifestHierarchy, Selectors,
)


def dummy_command_param():
    """Helper function: return a valid parsed command line.

    This is mostly useful for `compile_config` related tests.

    """
    return types.SimpleNamespace(
        parser="get",
        verbosity=9,
        folder="/tmp",
        kinds=["Deployment"],
        labels=[("app", "morty"), ("foo", "bar")],
        namespaces=["default"],
        kubeconfig="kubeconfig",
        ctx=None,
        groupby=None,
    )


class TestMain:
    def test_compile_config_basic(self):
        """Compile various valid configurations."""
        param = dummy_command_param()
        cfg, err = main.compile_config(param)
        assert not err
        assert cfg == Configuration(
            command='get', verbosity=9, folder=pathlib.Path('/tmp'),
            kinds=['Deployment'],
            namespaces=['default'],
            kubeconfig='kubeconfig', kube_ctx=None,
            selectors=Selectors(
                kinds=['Deployment'],
                namespaces=['default'],
                labels={("app", "morty"), ("foo", "bar")}
            ),
            groupby=ManifestHierarchy(label='', order=[]),
            # Must not populate Kubernetes data.
            k8s_config=K8sConfig(
                url=None, token=None, ca_cert=None,
                client_cert=None, version=None, name=None),
            k8s_client=None,
        )

    def test_compile_config_kinds(self):
        """Parse resource kinds."""
        # Specify `Service` twice.
        param = dummy_command_param()
        param.kinds = ["Service", "Deploy", "Service"]
        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.kinds == ["Service", "Deploy"]

        # The "all" resource must expand to all supported kinds.
        param.kinds = ["all"]
        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.kinds == list(SUPPORTED_KINDS)

        # Must remove duplicate resources.
        param.kinds = ["all", "svc", "all"]
        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.kinds == list(SUPPORTED_KINDS)

    def test_compile_config_k8s_credentials(self):
        """Parse K8s credentials."""
        # Must return error without K8s credentials.
        param = dummy_command_param()
        param.kubeconfig = None
        assert main.compile_config(param) == (None, True)

    def test_compile_hierarchy_ok(self):
        """Parse file system hierarchy."""
        param = dummy_command_param()

        # ----------------------------------------------------------------------
        # Default hierarchy.
        # ----------------------------------------------------------------------
        for cmd in ["apply", "get", "plan"]:
            param.parser = cmd
            ret, err = main.compile_config(param)
            assert not err
            assert ret.groupby == ManifestHierarchy(order=[], label="")
            del cmd, ret, err

        # ----------------------------------------------------------------------
        # User defined hierarchy with a valid label.
        # ----------------------------------------------------------------------
        param.parser = "get"
        param.groupby = ("ns", "kind", "label=app", "ns")
        ret, err = main.compile_config(param)
        assert not err
        assert ret.groupby == ManifestHierarchy(
            order=["ns", "kind", "label", "ns"], label="app")

        # ----------------------------------------------------------------------
        # User defined hierarchy with invalid labels.
        # ----------------------------------------------------------------------
        param.parser = "get"
        invalid_labels = ["label", "label=", "label=foo=bar"]
        for label in invalid_labels:
            param.groupby = ("ns", "kind", label, "ns")
            assert main.compile_config(param) == (None, True)

        # ----------------------------------------------------------------------
        # User defined hierarchy with invalid resource types.
        # ----------------------------------------------------------------------
        param.parser = "get"
        param.groupby = ("ns", "unknown")
        assert main.compile_config(param) == (None, True)

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
        config = K8sConfig("url", "token", "ca_cert", "client_cert", "1.10", "")

        # Default grouping because we will not specify custom ones in this test.
        groupby = ManifestHierarchy(order=[], label="")

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
                main.main()
            del args

        # Every main function must have been called exactly once.
        selectors = Selectors(["Deployment", "Service"], None, set())
        args = config, "client", pathlib.Path("myfolder"), selectors
        m_get.assert_called_once_with(*args, groupby)
        m_plan.assert_called_once_with(*args)
        m_apply.assert_called_once_with(*args, None, config.name)

    def test_main_version(self):
        """Simulate "version" command."""
        with mock.patch("sys.argv", ("square.py", "version")):
            assert main.main() == 0

    @mock.patch.object(square, "k8s")
    def test_main_invalid_option(self, m_k8s):
        """Simulate a missing or unknown option.

        Either way, the program must abort with a non-zero exit code.

        """
        # Do not pass any option.
        with mock.patch("sys.argv", ["square.py"]):
            with pytest.raises(SystemExit) as err:
                main.main()
            assert err.value.code == 2

        # Pass an invalid option.
        with mock.patch("sys.argv", ["square.py", "invalid-option"]):
            with pytest.raises(SystemExit) as err:
                main.main()
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
        `main.main` must return with a non-zero exit code.

        """
        # Dummy configuration.
        config = K8sConfig("url", "token", "ca_cert", "client_cert", "1.10", "")

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
                assert main.main() == 1

    @mock.patch.object(main, "parse_commandline_args")
    @mock.patch.object(square, "cluster_config")
    def test_main_invalid_option_in_main(self, m_cluster, m_cmd):
        """Simulate an option that `square` does not know about.

        This is a somewhat pathological test and exists primarily to close some
        harmless gaps in the unit test coverage.

        """
        # A valid Square configuration.
        cfg = Configuration(
            command='get', verbosity=9, folder=pathlib.Path('/tmp'),
            kinds=['Deployment'],
            namespaces=['default'],
            kubeconfig='kubeconfig', kube_ctx=None,
            selectors=Selectors(
                kinds=['Deployment'],
                namespaces=['default'],
                labels={("app", "morty"), ("foo", "bar")}
            ),
            groupby=ManifestHierarchy(label='', order=[]),
        )
        m_cluster.return_value = (cfg, False)

        # Force a configuration error in `compile_config` due to the missing
        # K8s credentials.
        cmd_args = dummy_command_param()
        cmd_args.kubeconfig = None
        m_cmd.return_value = cmd_args
        assert main.main() == 1

        # Simulate an invalid Square command.
        m_cmd.return_value = dummy_command_param()
        m_cluster.return_value = (cfg._replace(command="invalid"), False)
        assert main.main() == 1

    @mock.patch.object(square, "k8s")
    def test_main_version_error(self, m_k8s):
        """Program must abort if it cannot get the version from K8s."""
        # Mock all calls to the K8s API.
        m_k8s.version.return_value = (None, True)

        with mock.patch("sys.argv", ["square.py", "get", "deploy"]):
            assert main.main() == 1

    def test_parse_commandline_args_basic(self):
        """Must correctly expand eg "svc" -> "Service" and remove duplicates."""
        with mock.patch("sys.argv", ["square.py", "get", "deploy", "svc"]):
            ret = main.parse_commandline_args()
            assert ret.kinds == ["Deployment", "Service"]

    def test_parse_commandline_args_invalid(self):
        """An invalid resource name must abort the program."""
        with mock.patch("sys.argv", ["square.py", "get", "invalid"]):
            with pytest.raises(SystemExit):
                main.parse_commandline_args()

    def test_parse_commandline_args_labels_valid(self):
        """The labels must be returned as (name, value) tuples."""
        # No labels.
        with mock.patch("sys.argv", ["square.py", "get", "all"]):
            ret = main.parse_commandline_args()
            assert ret.labels == tuple()

        # One label.
        with mock.patch("sys.argv", ["square.py", "get", "all", "-l", "foo=bar"]):
            ret = main.parse_commandline_args()
            assert ret.labels == [("foo", "bar")]

        # Two labels.
        with mock.patch("sys.argv",
                        ["square.py", "get", "all", "-l", "foo=bar", "x=y"]):
            ret = main.parse_commandline_args()
            assert ret.labels == [("foo", "bar"), ("x", "y")]

    def test_parse_commandline_get_grouping(self):
        """The GET supports file hierarchy options."""
        # ----------------------------------------------------------------------
        # Default file system hierarchy.
        # ----------------------------------------------------------------------
        with mock.patch("sys.argv", ["square.py", "get", "all"]):
            param = main.parse_commandline_args()
            assert param.groupby is None

        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.groupby == ManifestHierarchy(label="", order=[])

        # ----------------------------------------------------------------------
        # User defined file system hierarchy.
        # ----------------------------------------------------------------------
        cmd = ["--groupby", "ns", "kind"]
        with mock.patch("sys.argv", ["square.py", "get", "all", *cmd]):
            param = main.parse_commandline_args()
            assert param.groupby == ["ns", "kind"]

        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.groupby == ManifestHierarchy(label="", order=["ns", "kind"])

        # ----------------------------------------------------------------------
        # Include a label into the hierarchy and use "ns" twice.
        # ----------------------------------------------------------------------
        cmd = ("--groupby", "ns", "label=foo", "ns")
        with mock.patch("sys.argv", ["square.py", "get", "all", *cmd]):
            param = main.parse_commandline_args()
            assert param.groupby == ["ns", "label=foo", "ns"]

        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.groupby == ManifestHierarchy(label="foo", order=["ns", "label", "ns"])

        # ----------------------------------------------------------------------
        # The label resource, unlike "ns" or "kind", can only be specified
        # at most once.
        # ----------------------------------------------------------------------
        cmd = ("--groupby", "ns", "label=foo", "label=bar")
        with mock.patch("sys.argv", ["square.py", "get", "all", *cmd]):
            param = main.parse_commandline_args()
            assert param.groupby == ["ns", "label=foo", "label=bar"]

        assert main.compile_config(param) == (None, True)

    def test_parse_commandline_args_labels_invalid(self):
        """Must abort on invalid labels."""
        invalid_labels = (
            "foo", "foo=", "=foo", "foo=bar=foobar", "foo==bar",
            "fo/o=bar",
        )
        for label in invalid_labels:
            with mock.patch("sys.argv", ["square.py", "get", "all", "-l", label]):
                with pytest.raises(SystemExit):
                    main.parse_commandline_args()

    def test_parse_commandline_args_kubeconfig(self):
        """Use the correct Kubeconfig file."""
        # Backup environment variables and set a custom KUBECONFIG value.
        new_env = os.environ.copy()

        # Populate the environment with a KUBECONFIG.
        new_env["KUBECONFIG"] = "envvar"
        with mock.patch.dict("os.environ", values=new_env, clear=True):
            # Square must use the supplied Kubeconfig file and ignore the
            # environment variable.
            with mock.patch(
                    "sys.argv",
                    ["square.py", "get", "svc", "--kubeconfig", "/file"]):
                ret = main.parse_commandline_args()
                assert ret.kubeconfig == "/file"

            # Square must fall back to the KUBECONFIG environment variable.
            with mock.patch("sys.argv", ["square.py", "get", "svc"]):
                ret = main.parse_commandline_args()
                assert ret.kubeconfig == "envvar"

        # Square must return `None` if there is neither a KUBECONFIG env var
        # nor a user specified argument.
        del new_env["KUBECONFIG"]
        with mock.patch.dict("os.environ", values=new_env, clear=True):
            with mock.patch("sys.argv", ["square.py", "get", "svc"]):
                ret = main.parse_commandline_args()
                assert ret.kubeconfig is None

    def test_parse_commandline_args_folder(self):
        """Use the correct manifest folder."""
        # Backup environment variables and set a custom SQUARE_FOLDER value.
        new_env = os.environ.copy()

        # Populate the environment with a SQUARE_FOLDER.
        new_env["SQUARE_FOLDER"] = "envvar"
        with mock.patch.dict("os.environ", values=new_env, clear=True):
            # Square must use the supplied value and ignore the environment variable.
            with mock.patch("sys.argv", ["square.py", "get", "svc", "--folder", "/tmp"]):
                ret = main.parse_commandline_args()
                assert ret.folder == "/tmp"

            # Square must fall back to SQUARE_FOLDER if it exists.
            with mock.patch("sys.argv", ["square.py", "get", "svc"]):
                ret = main.parse_commandline_args()
                assert ret.folder == "envvar"

        # Square must default to "./" in the absence of "--folder" and SQUARE_FOLDER.
        # parameters and environment variable.
        del new_env["SQUARE_FOLDER"]
        with mock.patch.dict("os.environ", values=new_env, clear=True):
            with mock.patch("sys.argv", ["square.py", "get", "svc"]):
                ret = main.parse_commandline_args()
                assert ret.folder == "./"

    def test_user_confirmed(self):
        """Verify user confirmation dialog."""
        # Disable dialog and assume a correct answer.
        assert square.user_confirmed(None) is True

        # Answer matches expected answer: must return True.
        with mock.patch.object(square, 'input', lambda _: "yes"):
            assert square.user_confirmed("yes") is True

        # Every other answer must return False.
        answers = ("YES", "", "y", "ye", "yess", "blah")
        for answer in answers:
            with mock.patch.object(square, 'input', lambda _: answer):
                assert square.user_confirmed("yes") is False

        # Must gracefully handle keyboard interrupts and return False.
        with mock.patch.object(square, 'input') as m_input:
            m_input.side_effect = KeyboardInterrupt
            assert square.user_confirmed("yes") is False
