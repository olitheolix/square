import copy
import sys
from pathlib import Path
from typing import Any

import pydantic
import pytest
import yaml

import square
import square.cfgfile as cfgfile
from square.dtypes import DEFAULT_PRIORITIES, Config


class TestLoadConfig:
    def test_load(self):
        """Load and parse configuration file."""
        # Load the sample that ships with Square.
        fname = Path("tests/support/config.yaml")
        cfg, err = cfgfile.load(fname)
        assert not err and isinstance(cfg, Config)

        assert cfg.folder == fname.parent.absolute() / "some/path"
        assert cfg.kubeconfig == Path("/path/to/kubeconfig")
        assert cfg.kubecontext is None
        assert cfg.priorities == list(DEFAULT_PRIORITIES)
        assert cfg.selectors.kinds == set(DEFAULT_PRIORITIES)
        assert cfg.selectors.namespaces == ["default", "kube-system"]
        assert cfg.selectors.labels == ["app=square"]
        assert set(cfg.filters.keys()) == {
            "_common_", "ConfigMap", "Deployment", "HorizontalPodAutoscaler", "Service"
        }

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
        ref["filters"]["_common_"].clear()
        fout = tmp_path / "corrupt.yaml"
        fout.write_text(yaml.dump(ref))

        # Load the new configuration. This must succeed and the filters must
        # match the ones defined in the file because there the "_common_"
        # filter was empty.
        cfg, err = cfgfile.load(fout)
        assert not err and ref["filters"] == cfg.filters

        # ----------------------------------------------------------------------
        # Missing _common_ filters.
        # ----------------------------------------------------------------------
        # Remove the "_common_" filter and save the configuration again.
        ref = yaml.safe_load(fname_ref.read_text())
        del ref["filters"]["_common_"]
        fout = tmp_path / "valid.yaml"
        fout.write_text(yaml.dump(ref))

        # Load the new configuration. This must succeed and the filters must
        # match the ones defined in the file because there was no "_common_"
        # filter to merge.
        cfg, err = cfgfile.load(fout)
        assert cfg.filters["_common_"] == []
        del cfg.filters["_common_"]
        assert not err and ref["filters"] == cfg.filters

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


class TestFilters:
    def test_valid(self):
        # Must be list.
        assert cfgfile.valid([]) is True

        # List must not contain empty strings.
        assert cfgfile.valid([""]) is False
        assert cfgfile.valid(["foo"]) is True

        # Dictionaries must have exactly one key.
        assert cfgfile.valid([{}]) is False
        assert cfgfile.valid([{"foo": "foo"}]) is False
        assert cfgfile.valid([{"foo": ["foo"]}]) is True
        assert cfgfile.valid([{"foo": "foo", "bar": "bar"}]) is False

        # List must only contain dictionaries and strings.
        assert cfgfile.valid(["foo"]) is True
        assert cfgfile.valid(["foo", {"bar": ["y"]}]) is True

        # Nested cases:
        assert cfgfile.valid(["foo", {"bar": ["bar"]}]) is True
        assert cfgfile.valid(["foo", {"bar": [{"x": ["x"]}]}]) is True
        assert cfgfile.valid(["foo", {"bar": [{"x": "x"}]}]) is False

        # Special case: the _`valid` must throw an error if it encounters
        # anything but a dict or str in the list of inputs.
        assert cfgfile._valid([{"must-be-dict-or-str"}]) is False

    def test_merge(self):
        defaults = copy.deepcopy(square.DEFAULT_CONFIG.filters["_common_"])
        assert cfgfile.merge(defaults, []) == defaults

        # Merge a `custom` filter into the default exclusion filters.
        custom = [{"foo": ["bar"]}]
        expected = [
            {"foo": ["bar"]},
            {"metadata": [
                {"annotations": [
                    "autoscaling.alpha.kubernetes.io/conditions",
                    "deployment.kubernetes.io/revision",
                    "kubectl.kubernetes.io/last-applied-configuration",
                    "kubernetes.io/change-cause",
                ]},
                "creationTimestamp",
                "generation",
                "managedFields",
                "resourceVersion",
                "selfLink",
                "uid",
            ]},
            "status",
        ]
        assert cfgfile.merge(defaults, custom) == expected

        # Repeat the test but with a more complex `custom` filter.
        custom = [
            {"metadata": [
                {"annotations": ["orig-annot"]},
                "orig-meta",
            ]},
        ]
        expected = [
            {"metadata": [
                {"annotations": [
                    "autoscaling.alpha.kubernetes.io/conditions",
                    "deployment.kubernetes.io/revision",
                    "kubectl.kubernetes.io/last-applied-configuration",
                    "kubernetes.io/change-cause",
                    "orig-annot",
                ]},
                "creationTimestamp",
                "generation",
                "managedFields",
                "orig-meta",
                "resourceVersion",
                "selfLink",
                "uid",
            ]},
            "status",
        ]
        assert cfgfile.merge(defaults, custom) == expected

        # Another one, this time a bit simpler than the previous one because
        # the `custom` filter for `metadata` just contains a list of strings.
        custom = [
            {"metadata": ["orig-meta"]},
        ]
        expected = [
            {"metadata": [
                {"annotations": [
                    "autoscaling.alpha.kubernetes.io/conditions",
                    "deployment.kubernetes.io/revision",
                    "kubectl.kubernetes.io/last-applied-configuration",
                    "kubernetes.io/change-cause",
                ]},
                "creationTimestamp",
                "generation",
                "managedFields",
                "orig-meta",
                "resourceVersion",
                "selfLink",
                "uid",
            ]},
            "status",
        ]
        assert cfgfile.merge(defaults, custom) == expected


class TestModels:
    def test_config(self):
        kwargs = dict(kubeconfig=Path(), folder=Path('/tmp'))

        # The top level entry must always be a dict.
        valid_filters: Any = (
            {"foo": []},
            {"foo": ["str"]},
            {"foo": ["str", {"bar": ["bar"]}]},
            {"foo": ["str", {"bar": [{"x": ["x"]}]}]},
            {"foo": ["str", {"bar": [{"x": ["x"]}, {"y": ["y"]}]}]},
            {"foo": ["str", {"bar": ["spam", {"x": ["x"]}]}]},
        )
        for filters in valid_filters:
            Config(**kwargs, filters=filters)  # type: ignore

        invalid_filters: Any = (
            # Must be a dict.
            [], set(), tuple(),

            # The dict value must be a List.
            {"foo": "bar"},
            {"foo": {"bar": ["bar"]}},

            # Each dict must have exactly one key.
            {"foo": [{}]},
            {"foo": [{"key1": [], "key2": []}]},

            # Dict keys and list entries must be non-empty strings.
            {"foo": [""]},
            {"foo": [{"": []}]},

            # Each list entry must be either a string or dict.
            {"foo": [{"bar": [set()]}]},
            {"foo": [{"bar": {"foo"}}]},

            # Dict keys must be non-empty strings.
            {"": []},
            {10: []},

            {"foo": [{"": []}]},
            {"foo": [{10: []}]},
        )

        for filters in invalid_filters:
            with pytest.raises(pydantic.ValidationError):
                Config(**kwargs, filters=filters)  # type: ignore
