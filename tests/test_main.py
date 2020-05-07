import copy
import os
import pathlib
import types
import unittest.mock as mock
from typing import Generator, Tuple

import pytest
import square.k8s as k8s
import square.main as main
import square.manio as manio
import square.square as square
import yaml
from square.dtypes import (
    DEFAULT_CONFIG_FILE, DEFAULT_PRIORITIES, Config, DeltaCreate, DeltaDelete,
    DeltaPatch, DeploymentPlan, Filepath, GroupBy, JsonPatch, Selectors,
)

from .test_helpers import make_manifest

# Location of test configuration.
TEST_CONFIG_FNAME = pathlib.Path(__file__).parent / "support/config.yaml"


@pytest.fixture
def fname_param_config(tmp_path) -> Generator[
        Tuple[Filepath, types.SimpleNamespace, Config], None, None]:
    """Parsed command line args to produce the default configuration.

    The return values are what `parse_commandline_args` would return, as well
    as what `compile_config` should convert them into.

    This removes a lot of boiler plate in the tests.

    """
    # Location of our configuration file and dummy Kubeconfig.
    fname_square = tmp_path / ".square.yaml"
    fname_kubeconfig = tmp_path / "kubeconfig-dot-square"

    # Duplicate the default configuration with the new kubeconfig.
    ref = yaml.safe_load(DEFAULT_CONFIG_FILE.read_text())
    assert "kubeconfig" in ref and "kubecontext" in ref
    ref["kubeconfig"] = str(fname_kubeconfig.absolute())

    # We will also "abuse" the `kubecontext` field to indicate that this file
    # is our mock ".square.yaml".
    ref["kubecontext"] = "dot-square"
    fname_square.write_text(yaml.dump(ref))
    del ref

    # Load the sample configuration.
    config, err = square.load_config(fname_square)
    assert not err

    # Point the folder and kubeconfig to temporary versions.
    config.folder = tmp_path

    # Ensure the dummy kubeconfig file exists.
    config.kubeconfig.write_text("")

    # Override the version because this one is difficult (and pointless) to
    # compare in tests.
    config.version = ""

    params = types.SimpleNamespace(
        # Not in config file - command line arguments only.
        parser="get",
        configfile="",
        verbosity=9,
        info=False,
        no_config=False,

        # Dummy kubeconfig created by the `config` fixture.
        kubeconfig=str(config.kubeconfig),

        # These were not specified on the command line.
        folder=".",
        kinds=DEFAULT_PRIORITIES,
        labels=[],
        namespaces=["default"],
        kubecontext=None,
        groupby=["ns", "label=app", "kind"],
        priorities=DEFAULT_PRIORITIES,
    )

    cwd = pathlib.Path.cwd()
    os.chdir(tmp_path)
    yield fname_square, params, config
    os.chdir(cwd)


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
    def test_compile_config_basic(self, fname_param_config):
        """Verify that our config and command line args fixtures match."""
        _, param, ref_config = fname_param_config

        # Convert the parsed command line `param` to a `Config` structure.
        out, err = main.compile_config(param)
        assert not err

        # The parsed `Config` must match our `ref_config` fixture except for
        # the folder and kubeconfig because the fixture explicitly overrode
        # those to point to a temporary location.
        out.folder, out.kubeconfig = ref_config.folder, ref_config.kubeconfig
        assert out == ref_config

    def test_compile_config_kinds(self, fname_param_config):
        """Parse resource kinds."""
        # Specify `Service` twice.
        _, param, ref_config = fname_param_config
        param.kinds = ["Service", "Deploy", "Service"]
        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.selectors.kinds == {"Service", "Deploy"}

        # An empty resource list must use the defaults.
        param.kinds = None
        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.selectors.kinds == ref_config.selectors.kinds

    def test_compile_config_kinds_merge_file(self, config, tmp_path):
        """Merge configuration from file and command line."""
        # Dummy file.
        kubeconfig_override = tmp_path / "kubeconfig"
        kubeconfig_override.write_text("")

        # ---------------------------------------------------------------------
        # Override nothing on the command line except for `kubeconfig` because
        # it must point to a valid file.
        # ---------------------------------------------------------------------
        param = types.SimpleNamespace(
            configfile=Filepath("tests/support/config.yaml"),

            # Must override this and point it to a dummy file or
            # `compile_config` will complain it does not exist.
            kubeconfig=str(kubeconfig_override),

            # User did not specify anything else.
            kubecontext=None,
            folder=None,
            groupby=None,
            kinds=None,
            labels=None,
            namespaces=None,
            priorities=None,
        )

        # Translate command line arguments into `Config`.
        cfg, err = main.compile_config(param)
        assert not err

        assert cfg.folder == Filepath("tests/support").absolute() / "some/path"
        assert cfg.kubeconfig == kubeconfig_override
        assert cfg.kubecontext is None
        assert cfg.priorities == list(DEFAULT_PRIORITIES)
        assert cfg.selectors == Selectors(
            kinds=set(DEFAULT_PRIORITIES),
            namespaces=["default", "kube-system"],
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
            kubecontext="kubecontext-override",
            groupby=["kind", "label=foo", "ns"],
            priorities=["Namespace", "Deployment"],
            configfile=Filepath("tests/support/config.yaml"),
        )

        # Translate command line arguments into `Config`.
        cfg, err = main.compile_config(param)
        assert not err

        assert cfg.folder == Filepath(param.folder)
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

    def test_compile_config_default_folder(self, fname_param_config):
        """Folder location.

        This is tricky because it depends on whether or not the user specified
        a config file and whether or not he also specified the "--folder" flag.

        """
        _, param, _ = fname_param_config

        # Config file and no "--folder": config file entry wins.
        param.configfile = str(TEST_CONFIG_FNAME)
        param.folder = None
        cfg, err = main.compile_config(param)
        assert not err
        assert cfg.folder == TEST_CONFIG_FNAME.parent.absolute() / "some/path"

        # Config file and "--folder some/where": `--folder` wins.
        param.configfile = str(TEST_CONFIG_FNAME)
        param.folder = "some/where"
        cfg, err = main.compile_config(param)
        assert not err and cfg.folder == Filepath("some/where")

        # No config file and no "--folder": manfiest folder must be CWD.
        param.configfile = None
        param.folder = None
        cfg, err = main.compile_config(param)
        assert not err and cfg.folder == Filepath.cwd()

        # No config file and "--folder some/where": must point to `some/where`.
        param.configfile = None
        param.folder = "some/where"
        cfg, err = main.compile_config(param)
        assert not err and cfg.folder == Filepath("some/where")

    def test_compile_config_dot_square(self, fname_param_config):
        """Folder location.

        This is tricky because it depends on whether or not the user specified
        a config file and whether or not he also specified the "--folder" flag.

        """
        _, param, _ = fname_param_config
        param.configfile = None

        assert Filepath(".square.yaml").exists()

        # User specified "--no-config": use `.square.yaml` if it exists.
        param.no_config = False
        cfg, err = main.compile_config(param)
        assert not err and cfg.kubecontext == "dot-square"

        # User did not specify "--no-config" and`.square.yaml` exists too. This must
        # still use the default configuration.
        param.no_config = True
        cfg, err = main.compile_config(param)
        assert not err and cfg.kubecontext is None

        # User did not specify "--no-config": use `.square.yaml` if it exists.
        Filepath(".square.yaml").rename(".square.yaml.bak")
        assert not Filepath(".square.yaml").exists()
        param.no_config = False
        cfg, err = main.compile_config(param)
        assert not err and cfg.kubecontext is None

    def test_compile_config_kubeconfig(self, fname_param_config, tmp_path):
        """Which kubeconfig to use.

        The tricky part here is when to use the KUBECONFIG env var. With
        Square, it will *only* try to use KUBECONFIG if neither `--config` nor
        `--kubeconfig` was specified.

        """
        # Unpack the fixture: path to valid ".square.yaml", command line
        # parameters and an already parsed `Config`.
        fname_config, param, ref_config = fname_param_config
        ref_data = yaml.safe_load(fname_config.read_text())

        cwd = fname_config.parent
        fname_config_dotsquare = fname_config
        fname_config_custom = cwd / "customconfig.yaml"

        fname_kubeconfig_dotsquare = cwd / "kubeconfig-dotsquare"
        fname_kubeconfig_custom = cwd / "kubeconfig-custom"
        fname_kubeconfig_commandline = cwd / "kubeconfig-commandline"
        fname_kubeconfig_envvar = cwd / "kubeconfig-envvar"

        def reset():
            """Reset the files in the temporary folder."""
            data = copy.deepcopy(ref_data)

            # Write ".square".
            data["kubeconfig"] = str(fname_kubeconfig_dotsquare)
            fname_config_dotsquare.write_text(yaml.dump(data))

            # Write "customconfig.yaml"
            data["kubeconfig"] = str(fname_kubeconfig_custom)
            fname_config_custom.write_text(yaml.dump(data))

            # Create empty files for all Kubeconfigs.
            fname_kubeconfig_dotsquare.write_text("")
            fname_kubeconfig_custom.write_text("")
            fname_kubeconfig_commandline.write_text("")
            fname_kubeconfig_envvar.write_text("")

            del data

        # Create dummy kubeconfig for when we want to simulate `--kubeconfig`.
        kubeconfig_file = tmp_path / "kubeconfig-commandline"
        kubeconfig_file.write_text("")

        for envvar in [True, False]:
            new_env = os.environ.copy()
            if envvar:
                new_env["KUBECONFIG"] = str(fname_kubeconfig_envvar)
            else:
                new_env.pop("KUBECONFIG", None)

            with mock.patch.dict("os.environ", values=new_env, clear=True):
                reset()

                # .square: true, custom: true, --kubeconf: true -> --kubeconf
                param.configfile = str(fname_config_custom)
                param.kubeconfig = str(fname_kubeconfig_commandline)
                cfg, err = main.compile_config(param)
                assert not err and cfg.kubeconfig == fname_kubeconfig_commandline

                # .square: true, custom: true, --kubeconf: false -> custom
                param.configfile = str(fname_config_custom)
                param.kubeconfig = None
                cfg, err = main.compile_config(param)
                assert not err and cfg.kubeconfig == fname_kubeconfig_custom

                # .square: true, custom: false, --kubeconf: true -> --kubeconf
                param.configfile = None
                param.kubeconfig = str(fname_kubeconfig_commandline)
                cfg, err = main.compile_config(param)
                assert not err and cfg.kubeconfig == fname_kubeconfig_commandline

                # .square: true, custom: false, --kubeconf: false -> dotsquare
                param.configfile = None
                param.kubeconfig = None
                cfg, err = main.compile_config(param)
                assert not err and cfg.kubeconfig == fname_kubeconfig_dotsquare

                # ------------------------------------------------------------------

                reset()
                fname_config_dotsquare.unlink()
                assert not fname_config_dotsquare.exists()

                # .square: false, custom: true, --kubeconf: true -> --kubeconf
                param.configfile = str(fname_config_custom)
                param.kubeconfig = str(fname_kubeconfig_commandline)
                cfg, err = main.compile_config(param)
                assert not err and cfg.kubeconfig == fname_kubeconfig_commandline

                # .square: false, custom: true, --kubeconf: false -> custom
                param.configfile = str(fname_config_custom)
                param.kubeconfig = None
                cfg, err = main.compile_config(param)
                assert not err and cfg.kubeconfig == fname_kubeconfig_custom

                # .square: false, custom: false, --kubeconf: true -> --kubeconf
                param.configfile = None
                param.kubeconfig = str(fname_kubeconfig_commandline)
                cfg, err = main.compile_config(param)
                assert not err and cfg.kubeconfig == fname_kubeconfig_commandline

                # .square: false, custom: false, --kubeconf: false -> env var.
                param.configfile = None
                param.kubeconfig = None
                cfg, err = main.compile_config(param)

                if envvar:
                    assert not err and cfg.kubeconfig == Filepath(os.getenv("KUBECONFIG"))
                else:
                    assert err

    def test_compile_config_kinds_clear_existing(self, fname_param_config, tmp_path):
        """Empty list on command line must clear the option."""
        _, param, _ = fname_param_config

        # Use the test configuration for this test (it has non-zero labels).
        param.configfile = str(TEST_CONFIG_FNAME)

        # User did not provide `--labels` or `--namespace` option.
        param.labels = None
        param.namespaces = None

        # Convert the parsed command line into a `Config` structure.
        cfg, err = main.compile_config(param)
        assert not err

        # The defaults must have taken over because the user did not specify
        # new labels etc.
        assert cfg.selectors.labels == {("app", "square")}
        assert cfg.selectors.namespaces == ["default", "kube-system"]
        assert cfg.groupby == GroupBy(label="app", order=["ns", "label", "kind"])

        # Pretend the user specified and empty `--labels`, `--groupby` and
        # `--namespaces`. This must clear the respective entries.
        param.labels = []
        param.groupby = []
        param.namespaces = []
        cfg, err = main.compile_config(param)
        assert not err

        # This time, the user supplied arguments must have cleared the
        # respective fields.
        assert cfg.selectors.labels == set()
        assert cfg.selectors.namespaces == []
        assert cfg.groupby == GroupBy()

    def test_compile_config_missing_config_file(self, fname_param_config):
        """Abort if the config file is missing or invalid."""
        _, param, _ = fname_param_config
        param.configfile = Filepath("/does/not/exist.yaml")
        _, err = main.compile_config(param)
        assert err

    def test_compile_config_missing_k8s_credentials(self, fname_param_config):
        """Gracefully abort if kubeconfig does not exist"""
        _, param, _ = fname_param_config
        param.kubeconfig += "does-not-exist"
        assert main.compile_config(param) == (
            Config(
                folder=Filepath(""),
                kubeconfig=Filepath(""),
                kubecontext=None,
                selectors=Selectors(set(), [], set()),
                groupby=GroupBy("", []),
                priorities=[],
            ), True)

    def test_compile_hierarchy_ok(self, fname_param_config):
        """Parse the `--groupby` argument."""
        _, param, _ = fname_param_config

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
                                tmp_path, fname_param_config, k8sconfig):
        """Simulate sane program invocation.

        This test verifies that the bootstrapping works and the correct
        `main_*` function will be called with the correct parameters.

        """
        _, _, config = fname_param_config
        m_cluster.side_effect = lambda *args: (k8sconfig, False)

        options = ["get", "plan", "apply"]

        # Pretend all functions return successfully.
        m_get.return_value = False
        m_plan.return_value = (None, False)
        m_apply.return_value = False

        # Simulate all input options.
        for option in options:
            args = (
                "square.py", option, *config.selectors.kinds,
                "--folder", str(config.folder),
                "--kubeconfig", str(config.kubeconfig),
                "--labels", "app=demo",
                "--namespace", "default",
            )
            with mock.patch("sys.argv", args):
                assert main.main() == 0
            del args

        # These two deviate from the values in `tests/support/config.yaml`.
        config.selectors.labels = {("app", "demo")}
        config.selectors.namespaces = ["default"]

        # Every main function must have been called exactly once.
        m_get.assert_called_once_with(config)
        m_apply.assert_called_once_with(config, "yes")
        m_plan.assert_called_once_with(config)

        # Repeat the tests but with the "--info" flag. This must not call any
        # functions.
        m_get.reset_mock()
        m_apply.reset_mock()
        m_plan.reset_mock()

        for option in options:
            args = (
                "square.py", option, *config.selectors.kinds,
                "--folder", str(config.folder),
                "--kubeconfig", str(config.kubeconfig),
                "--labels", "app=demo",
                "--namespace", "default",
                "--info",
            )
            with mock.patch("sys.argv", args):
                assert main.main() == 0
            del args

        # These two deviate from the values in `tests/support/config.yaml`.
        config.selectors.labels = {("app", "demo")}
        config.selectors.namespaces = ["default"]

        # Every main function must have been called exactly once.
        assert not m_get.called
        assert not m_apply.called
        assert not m_plan.called

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
    def test_main_invalid_option_in_main(self, m_cluster, m_cmd, k8sconfig,
                                         fname_param_config):
        """Simulate an option that `square` does not know about.

        This is a somewhat pathological test and exists primarily to close some
        harmless gaps in the unit test coverage.

        """
        _, param, _ = fname_param_config

        # Pretend the call to get K8s credentials succeeded.
        m_cluster.side_effect = lambda *args: (k8sconfig, False)

        # Simulate an invalid Square command.
        param.parser = "invalid"
        m_cmd.return_value = param
        assert main.main() == 1

        # Force a configuration error due to the absence of K8s credentials.
        param.kubeconfig += "does-not-exist"
        m_cmd.return_value = param
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
        assert DEFAULT_CONFIG_FILE.read_text() == fname.read_text()

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

        # Square must return `None` if there is neither a KUBECONFIG env var
        # nor a user specified argument.
        del new_env["KUBECONFIG"]
        with mock.patch.dict("os.environ", values=new_env, clear=True):
            with mock.patch("sys.argv", ["square.py", "get", "svc"]):
                ret = main.parse_commandline_args()
                assert ret.kubeconfig is None

    def test_parse_commandline_args_folder(self):
        """Use the correct manifest folder."""
        # Folder must be `None` by default. Square will interpolate later.
        with mock.patch("sys.argv", ["square.py", "get", "svc"]):
            ret = main.parse_commandline_args()
            assert ret.folder is None

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
