import os
import pathlib
import types
import unittest.mock as mock
from typing import Any, Tuple

import pytest
import square.k8s as k8s
import square.main as main
import square.manio as manio
import square.square as square
from square.dtypes import (
    DEFAULT_PRIORITIES, Config, DeltaCreate, DeltaDelete, DeltaPatch,
    DeploymentPlan, Filepath, GroupBy, JsonPatch, Selectors,
)

from .test_helpers import make_manifest


@pytest.fixture
def config_args(config) -> Tuple[Config, Any]:
    """Return a valid `Config` structure and associated `argparse` namespace.

    The purpose of this fixture is to supply a valid non-trivial configuration,
    as well as the command line arguments (ie `main.parise_commandline_args`
    return value) that would produce it.

    This removes a lot of boiler plate in the tests.

    """
    config.version = ""

    return config, types.SimpleNamespace(
        parser="get",
        verbosity=9,
        folder=None,
        kinds=config.selectors.kinds,
        labels=None,
        namespaces=None,
        kubeconfig=config.kubeconfig,
        ctx=None,
        groupby=None,
        priorities=None,
        config="",
    )


class TestResourceCleanup:
    @mock.patch.object(square.k8s, "cluster_config")
    def test_sanitise_resource_kinds(self, m_cluster, k8sconfig):
        """Must expand the short names if possible, and leave as is otherwise."""
        m_cluster.side_effect = lambda *args: (k8sconfig, False)

        # Use specified a valid set of `selectors.kinds` using various spellings.
        cfg = Config(
            folder=pathlib.Path('/tmp'),
            kubeconfig="",
            kubecontext=None,
            selectors=Selectors(
                kinds={"svc", 'DEPLOYMENT', "Secret"},
                namespaces=['default'],
                labels={("app", "morty"), ("foo", "bar")},
            ),
            groupby=GroupBy("", []),
            priorities=("Namespace", "Deployment"),
        )

        # Convert the resource names to their correct K8s kind.
        ret, err = main.sanitise_resource_kinds(cfg)
        assert not err and ret.selectors.kinds == {"Service", "Deployment", "Secret"}

        # Add two invalid resource names. This must succeed and return the
        # resource names unchanged.
        cfg.selectors.kinds.clear()
        cfg.selectors.kinds.update({"invalid", "k8s-resource-kind"})
        _, err = main.sanitise_resource_kinds(cfg)
        assert not err and ret.selectors.kinds == {"invalid", "k8s-resource-kind"}

    def test_sanitise_resource_kinds_err_config(self, k8sconfig):
        """Abort if the kubeconfig file does not exist."""
        cfg = Config(
            folder=pathlib.Path('/tmp'),
            kubeconfig=Filepath("/does/not/exist"),
            kubecontext=None,
            selectors=Selectors(
                kinds={"svc", 'DEPLOYMENT', "Secret"},
                namespaces=['default'],
                labels={("app", "morty"), ("foo", "bar")},
            ),
            groupby=GroupBy("", tuple()),
            priorities=("Namespace", "Deployment"),
        )

        _, err = main.sanitise_resource_kinds(cfg)
        assert err


class TestMain:
    def test_compile_config_basic(self, config_args):
        """Verify that our config and command line args fixtures match."""
        ref_config, param = config_args

        # Convert the parsed command line `param` to a `Config` structure.
        out, err =  main.compile_config(param)
        assert not err

        # The parsed `Config` must match our `ref_config` fixture except for
        # the folder and kubeconfig because the fixture explicitly overrode
        # those to point to a temporary location.
        out.folder, out.kubeconfig = ref_config.folder, ref_config.kubeconfig
        assert out == ref_config

    def test_compile_config_kinds(self, config_args):
        """Parse resource kinds."""
        # Specify `Service` twice.
        config, param = config_args
        param.kinds = ["Service", "Deploy", "Service"]
        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.selectors.kinds == {"Service", "Deploy"}

        # An empty resource list must expand to all supported kinds.
        param.kinds = []
        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.selectors.kinds == set()

    def test_compile_config_kinds_merge_file(self, config, tmp_path):
        """Merge configuration from file and command line."""
        # Dummy file.
        kubeconfig_override = tmp_path / "kubeconfig"
        kubeconfig_override.write_text("")

        # ---------------------------------------------------------------------
        # Override nothing on the command line except for `kubeconfig` because
        # it must point to valid file.
        # ---------------------------------------------------------------------
        param = types.SimpleNamespace(
            folder=None,
            kinds=None,
            labels=None,
            namespaces=None,
            kubeconfig=str(kubeconfig_override),
            ctx="",
            groupby=None,
            priorities=None,
            config="",
        )

        # Translate command line arguments into `Config`.
        param.config = Filepath("resources/sampleconfig.yaml")
        cfg, err = main.compile_config(param)
        assert not err

        assert cfg.folder == Filepath("resources/sampleconfig.yaml").parent.absolute()
        assert cfg.kubeconfig == kubeconfig_override
        assert cfg.kubecontext is None
        assert cfg.priorities == list(DEFAULT_PRIORITIES)
        assert cfg.selectors == Selectors(
            kinds=set(DEFAULT_PRIORITIES),
            namespaces=[],
            labels={("app", "square")},
        )
        assert cfg.groupby == GroupBy(label="app", order=["ns", "label", "kind"])
        assert set(cfg.filters.keys()) == {
            "ConfigMap", "Deployment", "HorizontalPodAutoscaler", "Service"
        }

        # ---------------------------------------------------------------------
        # Override everything on the command line.
        # ---------------------------------------------------------------------
        param = types.SimpleNamespace(
            folder="folder-override",
            kinds=["Deployment", "Namespace"],
            labels=[("app", "square"), ("foo", "bar")],
            namespaces=["default", "kube-system"],
            kubeconfig=str(kubeconfig_override),
            ctx="kubecontext-override",
            groupby=["kind", "label=foo", "ns"],
            priorities=["Namespace", "Deployment"],
            config="",
        )

        # Translate command line arguments into `Config`.
        param.config = Filepath("resources/sampleconfig.yaml")
        cfg, err = main.compile_config(param)
        assert not err

        assert cfg.folder == Filepath("folder-override")
        assert cfg.kubeconfig == kubeconfig_override
        assert cfg.kubecontext == "kubecontext-override"
        assert cfg.priorities == ["Namespace", "Deployment"]
        assert cfg.selectors == Selectors(
            kinds={"Namespace", "Deployment"},
            namespaces=["default", "kube-system"],
            labels={("app", "square"), ("foo", "bar")},
        )
        assert cfg.groupby == GroupBy(label="foo", order=["kind", "label", "ns"])
        assert set(cfg.filters.keys()) == {
            "ConfigMap", "Deployment", "HorizontalPodAutoscaler", "Service"
        }

    def test_compile_config_kinds_clear_existing(self, config_args, tmp_path):
        """Empty list on command line must clear the option."""
        config, param = config_args

        # The config sample defines a label.
        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.selectors.labels == {("app", "square")}
        assert cfg.groupby == GroupBy(label="app", order=["ns", "label", "kind"])

        # Pretend the user specified and empty `--labels` and `--groupby`. This
        # must clear both entries from the default configuration.
        param.labels = []
        param.groupby = []
        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.selectors.labels == set()
        assert cfg.groupby == GroupBy()

    def test_compile_config_missing_config_file(self, config_args):
        """Abort if the config file is missing or invalid."""
        config, param = config_args
        param.config = Filepath("/does/not/exist.yaml")
        _, err = main.compile_config(param)
        assert err

    def test_compile_config_k8s_credentials(self, config_args):
        """Parse K8s credentials."""
        # Must return error without K8s credentials.
        config, param = config_args
        param.kubeconfig /= "does-not-exist"
        assert main.compile_config(param) == (
            Config(
                folder=Filepath(""),
                kubeconfig=Filepath(""),
                kubecontext=None,
                selectors=Selectors(set(), [], set()),
                groupby=GroupBy("", []),
                priorities=[],
            ), True)

    def test_compile_hierarchy_ok(self, config_args):
        """Parse the `--groupby` argument."""
        config, param = config_args

        err_resp = Config(
            folder=Filepath(""),
            kubeconfig=Filepath(""),
            kubecontext=None,
            selectors=Selectors(set(), [], set()),
            groupby=GroupBy("", []),
            priorities=[],
        ), True

        # ----------------------------------------------------------------------
        # Default hierarchy.
        # ----------------------------------------------------------------------
        for cmd in ["apply", "get", "plan"]:
            param.parser = cmd
            ret, err = main.compile_config(param)
            assert not err
            assert ret.groupby == GroupBy(label="app", order=["ns", "label", "kind"])
            del cmd, ret, err

        # ----------------------------------------------------------------------
        # User defined hierarchy with a valid label.
        # ----------------------------------------------------------------------
        param.parser = "get"
        param.groupby = ("ns", "kind", "label=app", "ns")
        ret, err = main.compile_config(param)
        assert not err
        assert ret.groupby == GroupBy(label="app", order=["ns", "kind", "label", "ns"])

        # ----------------------------------------------------------------------
        # User defined hierarchy with invalid labels.
        # ----------------------------------------------------------------------
        param.parser = "get"
        invalid_labels = ["label", "label=", "label=foo=bar"]
        for label in invalid_labels:
            param.groupby = ("ns", "kind", label, "ns")
            assert main.compile_config(param) == err_resp

        # ----------------------------------------------------------------------
        # User defined hierarchy with invalid resource types.
        # ----------------------------------------------------------------------
        param.parser = "get"
        param.groupby = ("ns", "unknown")
        assert main.compile_config(param) == err_resp

    @mock.patch.object(square, "get_resources")
    @mock.patch.object(square, "make_plan")
    @mock.patch.object(main, "apply_plan")
    @mock.patch.object(square.k8s, "cluster_config")
    def test_main_valid_options(self, m_cluster, m_apply, m_plan, m_get,
                                tmp_path, config_args, k8sconfig):
        """Simulate sane program invocation.

        This test verifies that the bootstrapping works and the correct
        `main_*` function will be called with the correct parameters.

        """
        config, param = config_args
        m_cluster.side_effect = lambda *args: (k8sconfig, False)

        # Pretend all functions return successfully.
        m_get.return_value = (None, False)
        m_plan.return_value = (None, False)
        m_apply.return_value = (None, False)

        # Simulate all input options.
        for option in ["get", "plan", "apply"]:
            args = (
                "square.py", option, *config.selectors.kinds,
                "--folder", str(config.folder),
                "--kubeconfig", str(config.kubeconfig),
                "--labels", "app=demo",
                "--namespace", "default",
            )
            with mock.patch("sys.argv", args):
                main.main()
            del args

        # These two deviate from the values in `resources/sampleconfig.yaml`.
        config.selectors.labels = {("app", "demo")}
        config.selectors.namespaces = ["default"]

        # Every main function must have been called exactly once.
        m_get.assert_called_once_with(config)
        m_apply.assert_called_once_with(config, "yes")
        m_plan.assert_called_once_with(config)

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
    def test_main_nonzero_exit_on_error(self, m_apply, m_plan, m_get, m_k8s, k8sconfig):
        """Simulate sane program invocation.

        This test verifies that the bootstrapping works and the correct
        `main_*` function will be called with the correct parameters. However,
        each of those `main_*` functions returns with an error which means
        `main.main` must return with a non-zero exit code.

        """
        # Mock all calls to the K8s API.
        m_k8s.load_auto_config.return_value = k8sconfig
        m_k8s.session.return_value = "client"
        m_k8s.version.return_value = (k8sconfig, False)

        # Pretend all main functions return errors.
        m_get.return_value = (None, True)
        m_plan.return_value = (None, True)
        m_apply.return_value = (None, True)

        # Simulate all input options.
        for option in ["get", "plan", "apply"]:
            with mock.patch("sys.argv", ["square.py", option, "ns"]):
                assert main.main() == 1

    @mock.patch.object(main, "parse_commandline_args")
    @mock.patch.object(k8s, "cluster_config")
    def test_main_invalid_option_in_main(self, m_cluster, m_cmd, k8sconfig, config_args):
        """Simulate an option that `square` does not know about.

        This is a somewhat pathological test and exists primarily to close some
        harmless gaps in the unit test coverage.

        """
        config, cmd_args = config_args

        # Pretend the call to get K8s credentials succeeded.
        m_cluster.side_effect = lambda *args: (k8sconfig, False)

        # Force a configuration error due to the absence of K8s credentials.
        cmd_args.kubeconfig /= "does-not-exist"
        m_cmd.return_value = cmd_args
        assert main.main() == 1

        # Simulate an invalid Square command.
        cmd_args.parser = "invalid"
        m_cmd.return_value = cmd_args
        assert main.main() == 1

    @mock.patch.object(square, "k8s")
    def test_main_version_error(self, m_k8s):
        """Program must abort if it cannot get the version from K8s."""
        # Mock all calls to the K8s API.
        m_k8s.cluster_config.return_value = (None, True)

        with mock.patch("sys.argv", ["square.py", "get", "deploy"]):
            assert main.main() == 1

    def test_main_create_default_config_file(self, tmp_path):
        """Create a copy of the default config config in the specified folder."""
        folder = tmp_path / "folder"

        with mock.patch("sys.argv", ["square.py", "config", "--folder", str(folder)]):
            assert main.main() == 0

        fname = (folder / ".square.yaml")
        assert open("resources/sampleconfig.yaml").read() == fname.read_text()

    def test_parse_commandline_args_labels_valid(self):
        """The labels must be returned as (name, value) tuples."""
        # No labels.
        with mock.patch("sys.argv", ["square.py", "get", "all"]):
            ret = main.parse_commandline_args()
            assert ret.labels is None

        # One label.
        with mock.patch("sys.argv", ["square.py", "get", "all", "-l", "foo=bar"]):
            ret = main.parse_commandline_args()
            assert ret.labels == [("foo", "bar")]

        # Two labels.
        with mock.patch("sys.argv",
                        ["square.py", "get", "all", "-l", "foo=bar", "x=y"]):
            ret = main.parse_commandline_args()
            assert ret.labels == [("foo", "bar"), ("x", "y")]

    def test_parse_commandline_args_priority(self):
        """Custom priorities must override the default."""
        args = ["square.py", "get", "ns"]

        # User did not specify a priority.
        with mock.patch("sys.argv", args):
            ret = main.parse_commandline_args()
            assert ret.priorities is None

        # User did specify priorities.
        with mock.patch("sys.argv", args + ["--priorities", "foo", "bar"]):
            ret = main.parse_commandline_args()
            assert ret.priorities == ["foo", "bar"]

    def test_parse_commandline_get_grouping(self, tmp_path):
        """GET supports file hierarchy options."""
        kubeconfig = tmp_path / "kubeconfig.yaml"
        kubeconfig.write_text("")
        base_cmd = ("square.py", "get", "all",
                    "--kubeconfig", str(tmp_path / "kubeconfig.yaml"))

        # ----------------------------------------------------------------------
        # Default file system hierarchy.
        # ----------------------------------------------------------------------
        with mock.patch("sys.argv", base_cmd):
            param = main.parse_commandline_args()
            assert param.groupby is None

        # ----------------------------------------------------------------------
        # User defined file system hierarchy.
        # ----------------------------------------------------------------------
        cmd = ("--groupby", "ns", "kind")
        with mock.patch("sys.argv", base_cmd + cmd):
            param = main.parse_commandline_args()
            assert param.groupby == ["ns", "kind"]

        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.groupby == GroupBy(label="", order=["ns", "kind"])

        # ----------------------------------------------------------------------
        # Include a label into the hierarchy and use "ns" twice.
        # ----------------------------------------------------------------------
        cmd = ("--groupby", "ns", "label=foo", "ns")
        with mock.patch("sys.argv", base_cmd + cmd):
            param = main.parse_commandline_args()
            assert param.groupby == ["ns", "label=foo", "ns"]

        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.groupby == GroupBy(label="foo", order=["ns", "label", "ns"])

        # ----------------------------------------------------------------------
        # The label resource, unlike "ns" or "kind", can only be specified
        # at most once.
        # ----------------------------------------------------------------------
        cmd = ("--groupby", "ns", "label=foo", "label=bar")
        with mock.patch("sys.argv", base_cmd + cmd):
            param = main.parse_commandline_args()
            assert param.groupby == ["ns", "label=foo", "label=bar"]

        expected = Config(
            folder=Filepath(""),
            kubeconfig=Filepath(""),
            kubecontext=None,
            selectors=Selectors(set(), [], set()),
            groupby=GroupBy("", []),
            priorities=[],
        )
        assert main.compile_config(param) == (expected, True)

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
        # Square must default to "./" in the absence of "--folder".
        with mock.patch("sys.argv", ["square.py", "get", "svc"]):
            ret = main.parse_commandline_args()
            assert ret.folder == "./"

    def test_user_confirmed(self):
        """Verify user confirmation dialog."""
        # Disable dialog and assume a correct answer.
        assert main.user_confirmed(None) is True

        # Answer matches expected answer: must return True.
        with mock.patch.object(main, 'input', lambda _: "yes"):
            assert main.user_confirmed("yes") is True

        # Every other answer must return False.
        answers = ("YES", "", "y", "ye", "yess", "blah")
        for answer in answers:
            with mock.patch.object(main, 'input', lambda _: answer):
                assert main.user_confirmed("yes") is False

        # Must gracefully handle keyboard interrupts and return False.
        with mock.patch.object(main, 'input') as m_input:
            m_input.side_effect = KeyboardInterrupt
            assert main.user_confirmed("yes") is False


class TestApplyPlan:
    @mock.patch.object(square, "make_plan")
    @mock.patch.object(square, "apply_plan")
    def test_apply_plan(self, m_apply, m_plan, config):
        """Simulate a successful resource update (add, patch delete).

        To this end, create a valid (mocked) deployment plan, mock out all
        calls, and verify that all the necessary calls are made.

        The second part of the test simulates errors. This is not a separate
        test because it shares virtually all the boiler plate code.

        """
        fun = main.apply_plan

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
        m_apply.return_value = False

        # Function must not apply the plan without the user's confirmation.
        with mock.patch.object(main, 'input', lambda _: "no"):
            assert fun(config, "yes") is True
        assert not m_apply.called

        # Function must apply the plan if the user confirms it.
        with mock.patch.object(main, 'input', lambda _: "yes"):
            assert fun(config, "yes") is False
        m_apply.assert_called_once_with(config, plan)

        # Repeat with disabled security question.
        m_apply.reset_mock()
        assert fun(config, None) is False
        m_apply.assert_called_once_with(config, plan)

        # -----------------------------------------------------------------
        #                   Simulate An Empty Plan
        # -----------------------------------------------------------------
        # Function must not even ask for confirmation if the plan is empty.
        m_apply.reset_mock()
        m_plan.return_value = (DeploymentPlan(create=[], patch=[], delete=[]), False)

        with mock.patch.object(main, 'input', lambda _: "yes"):
            assert fun(config, "yes") is False
        assert not m_apply.called

        # -----------------------------------------------------------------
        #                   Simulate Error Scenarios
        # -----------------------------------------------------------------
        # Make `apply_plan` fail.
        m_plan.return_value = (plan, False)
        m_apply.return_value = (None, True)
        assert fun(config, None) is True
