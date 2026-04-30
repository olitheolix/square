from pathlib import Path
import pydantic
import pytest

from square.dtypes import (
    Config,
    MetaManifest,
    Selectors,
    SelKindGroupNames,
    SelKindGroupNames as SKGN,
)


class TestSelectors:
    def test_Selectors_ok(self):
        """Valid set of K8s kinds for Selectors.

        These tests all populate the model via the constructor.
        """
        # Defaults.
        sel = Selectors()
        assert sel.str_skgns == set()

        # Verify some valid cases to specify a resource kind.
        kinds = {"pod", "pod.v1", "pod.v1/name"}
        sel = Selectors(kinds=kinds)
        assert sel.kinds == kinds

        # Must have produced a MetaManifest for each specified resource.
        assert sel.str_skgns == {"pod", "pod.v1", "pod.v1/name"}

    def test_Selectors_modify_ok(self):
        """Modify the selectors via attribute assignment."""
        # Create default model.
        sel = Selectors()
        assert sel.kinds == set() and sel.str_skgns == set()

        # Assign directly to `.kinds` and verify this udpates `_metamanifests`.
        sel.kinds = {"x/y"}
        assert sel.kinds == {"x/y"}
        assert sel.str_skgns == {"x/y"}

        # Use setter methods like `clear` and `update`. This must update
        # `._metamanifests` as well.
        sel.kinds.clear()
        sel.kinds.update({"a/b"})
        assert sel.kinds == {"a/b"}
        assert sel.str_skgns == {"a/b"}

    def test_Selectors_err(self):
        """Various error scenarios of invalid resource kinds."""
        # Various cases of invalid "kind" names.
        invalid_kinds = [
            "",
            "/",
            "/bar",
            " /",
            "/ ",
            " / ",
            "foo / bar",
            "foo/ bar",
            "foo /bar",
            "/ foo / bar",
        ]

        for kind in invalid_kinds:
            with pytest.raises(ValueError):
                Selectors(kinds={kind}).str_skgns

    def test_SelKindGroupNames_valid(self):
        kgn = SelKindGroupNames(value="pod.v1/name-1")
        assert kgn.kind == "pod"
        assert kgn.group == "v1"
        assert kgn.name == "name-1"
        assert kgn.kind_group == "pod.v1"
        assert kgn.ns == ""
        assert "pod.v1/name-1" == str(kgn)
        assert SelKindGroupNames(value=str(kgn)) == kgn

        kgn = SelKindGroupNames(value="pod.v1/name-1", ns="ns")
        assert kgn.kind == "pod"
        assert kgn.group == "v1"
        assert kgn.name == "name-1"
        assert kgn.kind_group == "pod.v1"
        assert kgn.ns == "ns"
        assert "pod.v1/name-1" == str(kgn)
        assert SelKindGroupNames(value=str(kgn), ns="ns") == kgn

        kgn = SelKindGroupNames(value="pod.v1")
        assert kgn.kind == "pod"
        assert kgn.group == "v1"
        assert kgn.name == ""
        assert kgn.kind_group == "pod.v1"
        assert kgn.ns == ""
        assert "pod.v1" == str(kgn)
        assert SelKindGroupNames(value=str(kgn)) == kgn

        kgn = SelKindGroupNames(value="pod.foo-bar")
        assert kgn.kind == "pod"
        assert kgn.group == "foo-bar"
        assert kgn.name == ""
        assert kgn.kind_group == "pod.foo-bar"
        assert kgn.ns == ""
        assert "pod.foo-bar" == str(kgn)
        assert SelKindGroupNames(value=str(kgn)) == kgn

        value = "role.rbac.authorization.k8s.io"
        kgn = SelKindGroupNames(value=value)
        assert kgn.kind == "role"
        assert kgn.group == "rbac.authorization.k8s.io"
        assert kgn.name == ""
        assert kgn.kind_group == "role.rbac.authorization.k8s.io"
        assert kgn.ns == ""
        assert value == str(kgn)
        assert SelKindGroupNames(value=str(kgn)) == kgn

        kgn = SelKindGroupNames(value="pod")
        assert kgn.kind == "pod"
        assert kgn.group == ""
        assert kgn.name == ""
        assert kgn.kind_group == "pod"
        assert kgn.ns == ""
        assert "pod" == str(kgn)
        assert SelKindGroupNames(value=str(kgn)) == kgn

        kgn = SelKindGroupNames(value="pod/name")
        assert kgn.kind == "pod"
        assert kgn.group == ""
        assert kgn.name == "name"
        assert kgn.kind_group == "pod"
        assert kgn.ns == ""
        assert "pod/name" == str(kgn)
        assert SelKindGroupNames(value=str(kgn)) == kgn

        kgn = SelKindGroupNames(value="POD.GROUP/NAME")
        assert kgn.kind == "pod"
        assert kgn.group == "group"
        assert kgn.name == "name"
        assert kgn.kind_group == "pod.group"
        assert kgn.ns == ""
        assert "pod.group/name" == str(kgn)
        assert SelKindGroupNames(value=str(kgn)) == kgn

        value = "configmap.v1/kube-root-ca.crt"
        kgn = SelKindGroupNames(value=value)
        assert kgn.kind == "configmap"
        assert kgn.group == "v1"
        assert kgn.name == "kube-root-ca.crt"
        assert kgn.kind_group == "configmap.v1"
        assert kgn.ns == ""
        assert value == str(kgn)
        assert SelKindGroupNames(value=str(kgn)) == kgn

        # Some more obscure naming examples that do exist on the integration
        # test cluster.
        value = "clusterrole.rbac.authorization.k8s.io/kubeadm:get-nodes"
        kgn = SelKindGroupNames(value=value)
        assert kgn.kind == "clusterrole"
        assert kgn.group == "rbac.authorization.k8s.io"
        assert kgn.name == "kubeadm:get-nodes"
        assert kgn.kind_group == "clusterrole.rbac.authorization.k8s.io"
        assert kgn.ns == ""
        assert value == str(kgn)
        assert SelKindGroupNames(value=str(kgn)) == kgn

        value = "rolebinding.rbac.authorization.k8s.io/system:controller:token-cleaner"
        kgn = SelKindGroupNames(value=value)
        assert kgn.kind == "rolebinding"
        assert kgn.group == "rbac.authorization.k8s.io"
        assert kgn.name == "system:controller:token-cleaner"
        assert kgn.kind_group == "rolebinding.rbac.authorization.k8s.io"
        assert kgn.ns == ""
        assert value == str(kgn)
        assert SelKindGroupNames(value=str(kgn)) == kgn

        value = "role.rbac.authorization.k8s.io/system::leader-locking-kube-scheduler"
        kgn = SelKindGroupNames(value=value)
        assert kgn.kind == "role"
        assert kgn.group == "rbac.authorization.k8s.io"
        assert kgn.name == "system::leader-locking-kube-scheduler"
        assert kgn.kind_group == "role.rbac.authorization.k8s.io"
        assert kgn.ns == ""
        assert value == str(kgn)
        assert SelKindGroupNames(value=str(kgn)) == kgn

    def test_SelKindGroupNames_invalid(self):
        invalid = [
            "",  # empty
            " pod ",
            "pod / name",  # whitespace
            # "pod.apps./example",  # group ends in "/"
            "pod.v1/name1/name2",
            # "pod.apps!@#/example",  # invalid chars in group
            "pod.apps/",  # empty name
            # "pod-1.apps",            # empty name
            "/name",  # no kind
        ]
        for value in invalid:
            print(value)
            with pytest.raises(pydantic.ValidationError):
                SelKindGroupNames(value=value)

    def test_metamanifest(self):
        meta = MetaManifest("v1", "Pod", "ns", "name")
        assert meta.skgn() == SKGN(value="pod.v1/name", ns="ns")

        meta = MetaManifest("", "Pod", "ns", "name")
        assert meta.skgn() == SKGN(value="pod/name", ns="ns")

        meta = MetaManifest("v1", "Pod", "ns", "")
        assert meta.skgn() == SKGN(value="pod.v1", ns="ns")

        meta = MetaManifest("", "Pod", None, "")
        assert meta.skgn() == SKGN(value="pod", ns="")


class TestConfigFilters:
    """Tests for the filters field validator on Config."""

    def make_config(self, filters):
        """Helper to construct a minimal Config with the given filters."""
        return Config(
            folder=Path("/tmp"),
            kubeconfig=Path("/tmp/kubeconfig"),
            filters=filters,
        )

    def test_filters_empty(self):
        """An empty filters dict is valid."""
        cfg = self.make_config({})
        assert cfg.filters == {}

    def test_filters_valid_simple(self):
        """Valid filters with simple JSON paths."""
        filters = {
            "deployment": ["metadata.labels", "spec.replicas"],
            "service": ["spec.clusterIP"],
        }
        cfg = self.make_config(filters)
        assert cfg.filters == filters

    def test_filters_valid_lowercase_manual_assign(self):
        """The filters must be lower-cased upon import as well as when directly
        assigned to the `.filters` attribute.

        """
        # Import.
        cfg = self.make_config({"DEPLOYMENT": [], "Service": []})
        assert cfg.filters == {"deployment": [], "service": []}

        # Assign values directly.
        cfg.filters = {"FOO": [], "BAR": []}
        assert cfg.filters == {"foo": [], "bar": []}

    def test_filters_valid_empty_path_list(self):
        """A valid key with an empty list of paths is allowed."""
        cfg = self.make_config({"deployment": []})
        assert cfg.filters == {"deployment": []}

    def test_filters_valid_complex_paths(self):
        """Valid filters with array wildcard and nested paths."""
        filters = {
            "deployment": [
                "spec.containers[*].env",
                "metadata.annotations['kubernetes.io/last-applied-configuration']",
            ],
        }
        cfg = self.make_config(filters)
        assert cfg.filters == filters

    def test_filters_invalid_key(self):
        """An invalid SelKindGroupNames key must raise a ValidationError."""
        invalid_keys = [
            "",  # empty string
            "/name",  # no kind
            "pod.apps/",  # empty name after slash
        ]
        for key in invalid_keys:
            with pytest.raises(pydantic.ValidationError):
                self.make_config({key: [".metadata.name"]})

    def test_filters_invalid_path(self):
        """An invalid JSON path must raise a ValidationError."""
        with pytest.raises(pydantic.ValidationError):
            self.make_config({"deployment": ["]$0metadata.labels"]})

    def test_filters_valid_key_with_group(self):
        """A key with an explicit API group is valid."""
        res = "role.rbac.authorization.k8s.io"
        cfg = self.make_config({res: ["metadata.labels"]})
        assert "metadata.labels" in cfg.filters[res]
