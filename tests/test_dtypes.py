import pytest

from square.dtypes import KindName, Selectors


class TestSelectors:
    def test_Selectors_ok(self):
        """Valid set of K8s kinds for Selectors.

        These tests all populate the model via the constructor.
        """
        # Defaults.
        sel = Selectors()
        assert sel.kinds == sel._kinds_only == set()
        assert sel._kinds_names == []

        # Verify the three valid cases to specify a resource kind.
        kinds = {"pod", "namespace/", "deploy/app1"}
        sel = Selectors(kinds=kinds)
        assert sel.kinds == kinds

        # Must not include the `deploy` because it specifies a specific
        # resource instead of just a generic KIND.
        assert sel._kinds_only == {"pod", "namespace"}

        # The unpacked `_kinds_names` must be in alphabetical order.
        assert sel._kinds_names == [
            KindName(kind="deploy", name="app1"),
            KindName(kind="namespace", name=""),
            KindName(kind="pod", name=""),
        ]

    def test_Selectors_kinds_only(self):
        """Must extract the correct set of K8s resource KINDS."""
        # Defaults.
        sel = Selectors()
        assert sel.kinds == sel._kinds_only == set()
        assert sel._kinds_names == []

        # Select all pods, a specific pod and a specific deployment. The model
        # must correctly unpack the resource KINDS, but most notably the only
        # generic resource kind is the `Pod` because `pod/app1` and
        # `deploy/app2` specify one explicit resource instead of an entire KIND.
        kinds = {"pod", "pod/app1", "deploy/app2"}
        sel = Selectors(kinds=kinds)
        assert sel.kinds == kinds
        assert sel._kinds_only == {"pod"}

        # The unpacked `_kinds_names` must be in alphabetical order.
        assert sel._kinds_names == [
            KindName(kind="deploy", name="app2"),
            KindName(kind="pod", name=""),
            KindName(kind="pod", name="app1"),
        ]

    def test_Selectors_modify_ok(self):
        """Modify the selectors via attribute assignment."""
        # Create default model.
        sel = Selectors()
        assert sel.kinds == set() and sel._kinds_names == []

        # Assign to the `kinds` attribute directly. This must also update the
        # `_kinds_names` attribute.
        sel.kinds = {"x/y"}
        assert sel.kinds == {"x/y"}
        assert sel._kinds_names == [KindName(kind="x", name="y")]

        # Use set methods like to `clear` and `update`. This must update
        # the `_kinds_names` attribute as well.
        sel.kinds.clear()
        sel.kinds.update({"a/b"})
        assert sel.kinds == {"a/b"}
        assert sel._kinds_names == [KindName(kind="a", name="b")]

    def test_Selectors_duplicates(self):
        """Must remove duplicate kinds after removing white space."""
        # These all specify the exact same resource kind and the model must be
        # smart enough to collapse them into a single one.
        sel = Selectors(kinds={"foo/bar", " foo/bar", "foo/bar ", " foo/bar "})
        assert sel.kinds == {"foo/bar"}
        assert sel._kinds_names == [KindName(kind="foo", name="bar")]

        sel = Selectors(kinds={"foo/", " foo/", "foo/ ", " foo/ "})
        assert sel.kinds == {"foo/"}
        assert sel._kinds_names == [KindName(kind="foo", name="")]

        sel = Selectors(kinds={"foo", " foo", "foo ", " foo "})
        assert sel.kinds == {"foo"}
        assert sel._kinds_names == [KindName(kind="foo", name="")]

    def test_Selectors_err(self):
        """Various error scenarios of invalid resource kinds."""
        # Various cases of invalid "kind" names.
        invalid_kinds = [
            "", "/", "/bar",
            " /", "/ ", " / "
            "foo / bar", "foo/ bar", "foo /bar"
        ]

        for kind in invalid_kinds:
            with pytest.raises(ValueError):
                Selectors(kinds={kind})._kinds_names
