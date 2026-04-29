import sys
from pathlib import Path

import yaml

import square
import square.callbacks
import square.cfgfile as cfgfile
from square.dtypes import (
    DEFAULT_PRIORITIES,
    Config,
    ConnectionParameters,
    GroupBy,
)


class TestLoadConfig:
    def test_connection_parameters(self):
        """Validate all the defaults."""
        conparam = ConnectionParameters()
        assert conparam.connect == 5
        assert conparam.read == 5
        assert conparam.write == 5
        assert conparam.pool == 5

        assert conparam.max_connections is None
        assert conparam.max_keepalive_connections is None
        assert conparam.keepalive_expiry == 5.0
        assert conparam.http1 is True
        assert conparam.http2 is True

    def test_config(self, tmp_path: Path):
        """Create `Config` instance."""
        # Minimal required arguments to construct `Config` instance.
        kubeconfig_path = tmp_path / "config.yaml"
        cfg = Config(kubeconfig=kubeconfig_path, folder=tmp_path)

        # Verify the defaults.
        assert cfg.kubeconfig == kubeconfig_path
        assert cfg.kubecontext is None
        assert cfg.selectors.kinds == set()
        assert cfg.selectors.namespaces == []
        assert cfg.selectors.labels == []
        assert cfg.priorities == list(DEFAULT_PRIORITIES)
        assert cfg.groupby == GroupBy()
        assert cfg.filters2 == {}
        assert cfg.strip_callback is square.callbacks.strip_manifest
        assert cfg.patch_callback is square.callbacks.patch_manifests
        assert cfg.user_data is None
        assert cfg.connection_parameters == ConnectionParameters()

    def test_config_bug_callbacks(self, tmp_path: Path):
        """Pass explicit callbacks functions to `Config`.

        This used to be a bug.

        """

        def cb_strip(_: Config, manifest: dict):
            return manifest

        def cb_patch(_: Config, l_manifest: dict, s_manifest: dict):
            return l_manifest, s_manifest

        # Pass explicit callbacks.
        cfg = Config(
            kubeconfig=tmp_path / "config.yaml",
            folder=tmp_path,
            strip_callback=cb_strip,
            patch_callback=cb_patch,
        )
        assert cfg.strip_callback == cb_strip
        assert cfg.patch_callback == cb_patch

        # Assign new callbacks to an existing `Config` instance.
        cfg.strip_callback = square.callbacks.strip_manifest
        cfg.patch_callback = square.callbacks.patch_manifests

        assert cfg.strip_callback == square.callbacks.strip_manifest
        assert cfg.patch_callback == square.callbacks.patch_manifests

    def test_load(self):
        """Load and parse configuration file."""
        # Load test configuration.
        fname = Path("tests/support/config.yaml")
        cfg, err = cfgfile.load(fname)
        assert not err and isinstance(cfg, Config)

        assert cfg.connection_parameters.disable_x509_strict is True
        assert cfg.connection_parameters.k8s_extra_headers == {"foo": "bar"}

        assert cfg.folder == fname.parent.absolute() / "some/path"
        assert cfg.kubeconfig == Path("/path/to/kubeconfig")
        assert cfg.kubecontext is None
        assert cfg.priorities == list(DEFAULT_PRIORITIES)
        assert cfg.selectors.kinds == set(DEFAULT_PRIORITIES)
        assert cfg.selectors.namespaces == ["default", "kube-system"]
        assert cfg.selectors.labels == ["app=square"]
        assert set(cfg.filters2.keys()) == {
            "_common_",
            "configmap",
            "deployment.apps",
            "horizontalpodautoscaler.autoscaling",
            "service",
        }
        assert cfg.strip_callback is square.callbacks.strip_manifest
        assert cfg.patch_callback is square.callbacks.patch_manifests

    def test_load_folder_paths(self, tmp_path):
        """The folder paths must always be relative to the config file."""
        fname = tmp_path / ".square.yaml"
        fname_ref = Path("tests/support/config.yaml")

        # The parsed folder must point to "tmp_path".
        ref = yaml.safe_load(fname_ref.read_text())
        fname.write_text(yaml.dump(ref))
        cfg, err = cfgfile.load(fname)
        assert not err and cfg.folder == tmp_path / "some/path"

        # The parsed folder must point to "tmp_path/folder".
        ref = yaml.safe_load(fname_ref.read_text())
        ref["folder"] = "my-folder"
        fname.write_text(yaml.dump(ref))
        cfg, err = cfgfile.load(fname)
        assert not err and cfg.folder == tmp_path / "my-folder"

        # An absolute path must ignore the position of ".square.yaml".
        # No idea how to handle this on Windows.
        if not sys.platform.startswith("win"):
            ref = yaml.safe_load(fname_ref.read_text())
            ref["folder"] = "/absolute/path"
            fname.write_text(yaml.dump(ref))
            cfg, err = cfgfile.load(fname)
            assert not err and cfg.folder == Path("/absolute/path")

    def test_common_filters(self, tmp_path):
        """Deal with empty or non-existing `_common_` filter."""
        fname_ref = Path("tests/support/config.yaml")

        # ----------------------------------------------------------------------
        # Empty _common_ filters.
        # ----------------------------------------------------------------------
        # Clear the "_common_" filter and save the configuration again.
        ref = yaml.safe_load(fname_ref.read_text())
        ref["filters2"]["_common_"].clear()
        fout = tmp_path / "corrupt.yaml"
        fout.write_text(yaml.dump(ref))

        # Load the new configuration. This must succeed and the filters must
        # match the ones defined in the file because there the "_common_"
        # filter was empty.
        cfg, err = cfgfile.load(fout)
        assert not err and set(ref["filters2"]) == set(cfg.filters2)

        # ----------------------------------------------------------------------
        # Missing _common_ filters.
        # ----------------------------------------------------------------------
        # Remove the "_common_" filter and save the configuration again.
        ref = yaml.safe_load(fname_ref.read_text())
        del ref["filters2"]["_common_"]
        fout = tmp_path / "valid.yaml"
        fout.write_text(yaml.dump(ref))

        # Load the new configuration. This must succeed and the filters must
        # match the ones defined in the file because there was no "_common_"
        # filter to merge.
        cfg, err = cfgfile.load(fout)
        assert "_common_" not in cfg.filters2
        assert not err and set(ref["filters2"]) == set(cfg.filters2)

    def test_load_err(self, tmp_path):
        """Gracefully handle missing file, corrupt content etc."""
        # Must gracefully handle a corrupt configuration file.
        fname = tmp_path / "does-not-exist.yaml"
        _, err = cfgfile.load(fname)
        assert err

        # YAML error.
        fname = tmp_path / "corrupt-yaml.yaml"
        fname.write_text("[foo")
        _, err = cfgfile.load(fname)
        assert err

        # Does not match the definition of `dtypes.Config`.
        fname = tmp_path / "invalid-pydantic-schema.yaml"
        fname.write_text("foo: bar")
        _, err = cfgfile.load(fname)
        assert err

        # YAML file is valid but not a map. This special case is important
        # because the test function will expand the content as **kwargs.
        fname = tmp_path / "invalid-pydantic-schema.yaml"
        fname.write_text("")
        _, err = cfgfile.load(fname)
        assert err

        # Load the sample configuration and corrupt the label selector. Instead
        # of a list of 2-tuples we make it a list of 3-tuples.
        cfg = yaml.safe_load(Path("tests/support/config.yaml").read_text())
        cfg["selectors"]["labels"] = [["foo", "bar", "foobar"]]
        fout = tmp_path / "corrupt.yaml"
        fout.write_text(yaml.dump(cfg))
        _, err = cfgfile.load(fout)
        assert err


class TestModels:
    def test_config_callbacks(self, tmp_path):
        """Verify that default callbacks were setup correctly."""
        cfg = Config(kubeconfig=Path(tmp_path), folder=tmp_path)
        assert cfg.patch_callback == square.callbacks.patch_manifests
        assert cfg.strip_callback == square.callbacks.strip_manifest
