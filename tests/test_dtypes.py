import pydantic
import pytest

from square.dtypes import (
    MetaManifest, Selectors, SelKindGroupNames, SelKindGroupNames as SKGN,
)


class TestSelectors:
    def test_Selectors_ok(self):
        """Valid set of K8s kinds for Selectors.

        These tests all populate the model via the constructor.
        """
        # Defaults.
        sel = Selectors()
        assert sel._metamanifests == set()

        # Verify some valid cases to specify a resource kind.
        kinds = {"pod", "pod.v1", "pod.v1/name"}
        sel = Selectors(kinds=kinds)
        assert sel.kinds == kinds

        # Must have produced a MetaManifest for each specified resource.
        assert sel._metamanifests == {"pod", "pod.v1", "pod.v1/name"}

    def test_Selectors_modify_ok(self):
        """Modify the selectors via attribute assignment."""
        # Create default model.
        sel = Selectors()
        assert sel.kinds == set() and sel._metamanifests == set()

        # Assign directly to `.kinds` and verify this udpates `_metamanifests`.
        sel.kinds = {"x/y"}
        assert sel.kinds == {"x/y"}
        assert sel._metamanifests == {"x/y"}

        # Use setter methods like `clear` and `update`. This must update
        # `._metamanifests` as well.
        sel.kinds.clear()
        sel.kinds.update({"a/b"})
        assert sel.kinds == {"a/b"}
        assert sel._metamanifests == {"a/b"}

    def test_Selectors_err(self):
        """Various error scenarios of invalid resource kinds."""
        # Various cases of invalid "kind" names.
        invalid_kinds = [
            "", "/", "/bar",
            " /", "/ ", " / ",
            "foo / bar", "foo/ bar", "foo /bar",
            "/ foo / bar",
        ]

        for kind in invalid_kinds:
            with pytest.raises(ValueError):
                Selectors(kinds={kind})._metamanifests

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
            "",                 # empty
            " pod ", "pod / name",  # whitespace
            # "pod.apps./example",  # group ends in "/"
            "pod.v1/name1/name2",
            # "pod.apps!@#/example",  # invalid chars in group
            "pod.apps/",            # empty name
            # "pod-1.apps",            # empty name
            "/name",            # no kind
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

        meta = MetaManifest("", "Pod", "", "")
        assert meta.skgn() == SKGN(value="pod", ns="")
