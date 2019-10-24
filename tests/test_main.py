import os
import pathlib
import types
import unittest.mock as mock

import pytest
import square.main as main
import square.manio as manio
import square.square as square
from square.dtypes import (
    SUPPORTED_KINDS, Configuration, DeltaCreate, DeltaDelete, DeltaPatch,
    DeploymentPlan, JsonPatch, K8sConfig, ManifestHierarchy, Selectors,
)

from .test_helpers import make_manifest


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
        assert cfg.selectors.kinds == ["Service", "Deploy"]

        # The "all" resource must expand to all supported kinds.
        param.kinds = ["all"]
        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.selectors.kinds == list(SUPPORTED_KINDS)

        # Must remove duplicate resources.
        param.kinds = ["all", "svc", "all"]
        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.selectors.kinds == list(SUPPORTED_KINDS)

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

    @mock.patch.object(square, "get_resources")
    @mock.patch.object(square, "make_plan")
    @mock.patch.object(main, "apply_plan")
    def test_main_valid_options(self, m_apply, m_plan, m_get):
        """Simulate sane program invocation.

        This test verifies that the bootstrapping works and the correct
        `main_*` function will be called with the correct parameters.

        """
        # Default grouping because we will not specify custom ones in this test.
        groupby = ManifestHierarchy(order=[], label="")

        # Pretend all functions return successfully.
        m_get.return_value = (None, False)
        m_plan.return_value = (None, False)
        m_apply.return_value = (None, False)

        # Simulate all input options.
        for option in ["get", "plan", "apply"]:
            args = (
                "square.py", option,
                "deployment", "service", "--folder", "myfolder",
                "--kubeconfig", "/foo"
            )
            with mock.patch("sys.argv", args):
                main.main()
            del args

        # Every main function must have been called exactly once.
        selectors = Selectors(["Deployment", "Service"], None, set())
        args = "/foo", None, pathlib.Path("myfolder"), selectors
        m_get.assert_called_once_with(*args, groupby)
        m_apply.assert_called_once_with(*args, "yes")
        m_plan.assert_called_once_with(*args)

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
    @mock.patch.object(square, "get_resources")
    @mock.patch.object(square, "make_plan")
    @mock.patch.object(square, "apply_plan")
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
        # Pretend the call to get K8s credentials succeeded.
        m_cluster.return_value = (("foo", "bar"), False)

        # Force a configuration error due to the absence of K8s credentials.
        cmd_args = dummy_command_param()
        cmd_args.kubeconfig = None
        m_cmd.return_value = cmd_args
        assert main.main() == 1

        # Simulate an invalid Square command.
        cmd_args = dummy_command_param()
        cmd_args.parser = "invalid"
        m_cmd.return_value = cmd_args
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


class TestApplyPlan:
    @mock.patch.object(square, "make_plan")
    @mock.patch.object(square, "apply_plan")
    def test_apply_plan(self, m_apply, m_plan, tmp_path):
        """Simulate a successful resource update (add, patch delete).

        To this end, create a valid (mocked) deployment plan, mock out all
        calls, and verify that all the necessary calls are made.

        The second part of the test simulates errors. This is not a separate
        test because it shares virtually all the boiler plate code.

        """
        fun = main.apply_plan
        selectors = Selectors(["kinds"], ["ns"], {("foo", "bar"), ("x", "y")})

        # -----------------------------------------------------------------
        #                   Simulate A Non-Empty Plan
        # -----------------------------------------------------------------
        # Valid Patch.
        patch = JsonPatch(
            url="patch_url",
            ops=[
                {'op': 'remove', 'path': '/metadata/labels/old'},
                {'op': 'add', 'path': '/metadata/labels/new', 'value': 'new'},
            ],
        )
        # Valid non-empty deployment plan.
        meta = manio.make_meta(make_manifest("Deployment", "ns", "name"))
        plan = DeploymentPlan(
            create=[DeltaCreate(meta, "create_url", "create_man")],
            patch=[DeltaPatch(meta, "diff", patch)],
            delete=[DeltaDelete(meta, "delete_url", "delete_man")],
        )

        # Simulate a none empty plan and successful application of that plan.
        m_plan.return_value = (plan, False)
        m_apply.return_value = (None, False)

        # Function must not apply the plan without the user's confirmation.
        with mock.patch.object(square, 'input', lambda _: "no"):
            assert fun("kubeconfig", None, tmp_path, selectors, "yes") == (None, True)
        assert not m_apply.called

        # Function must apply the plan if the user confirms it.
        with mock.patch.object(square, 'input', lambda _: "yes"):
            assert fun("kubeconfig", None, tmp_path, selectors, "yes") == (None, False)
        m_apply.assert_called_once_with("kubeconfig", None, plan)

        # Repeat with disabled security question.
        m_apply.reset_mock()
        assert fun("kubeconfig", None, tmp_path, selectors, None) == (None, False)
        m_apply.assert_called_once_with("kubeconfig", None, plan)

        # -----------------------------------------------------------------
        #                   Simulate An Empty Plan
        # -----------------------------------------------------------------
        # Function must not even ask for confirmation if the plan is empty.
        m_apply.reset_mock()
        m_plan.return_value = (DeploymentPlan(create=[], patch=[], delete=[]), False)

        with mock.patch.object(square, 'input', lambda _: "yes"):
            assert fun("kubeconfig", None, tmp_path, selectors, "yes") == (None, False)
        assert not m_apply.called

        # -----------------------------------------------------------------
        #                   Simulate Error Scenarios
        # -----------------------------------------------------------------
        # Make `apply_plan` fail.
        m_plan.return_value = (plan, False)
        m_apply.return_value = (None, True)
        assert fun("kubeconfig", None, tmp_path, selectors, None) == (None, True)
