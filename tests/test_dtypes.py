import pytest

from square.dtypes import MetaManifest, Selectors


class TestSelectors:
    def test_Selectors_ok(self):
        """Valid set of K8s kinds for Selectors.

        These tests all populate the model via the constructor.
        """
        # Defaults.
        sel = Selectors()
        assert sel._metamanifests == set()

        # Verify the three valid cases to specify a resource kind.
        kinds = {"pod", "namespace/", "deploy/app1"}
        sel = Selectors(kinds=kinds)
        assert sel.kinds == kinds

        # Must have produced a MetaManifest for each specified resource.
        assert sel._metamanifests == {
            MetaManifest(apiVersion="", kind="pod", namespace="", name=""),
            MetaManifest(apiVersion="", kind="namespace", namespace="", name=""),
            MetaManifest(apiVersion="", kind="deploy", namespace="", name="app1"),
        }

    def test_Selectors_modify_ok(self):
        """Modify the selectors via attribute assignment."""
        # Create default model.
        sel = Selectors()
        assert sel.kinds == set() and sel._metamanifests == set()

        # Assign directly to `.kinds` and verify this udpates `_metamanifests`.
        sel.kinds = {"x/y"}
        assert sel.kinds == {"x/y"}
        assert sel._metamanifests == {MetaManifest("", "x", "", "y")}

        # Use setter methods like `clear` and `update`. This must update
        # `._metamanifests` as well.
        sel.kinds.clear()
        sel.kinds.update({"a/b"})
        assert sel.kinds == {"a/b"}
        assert sel._metamanifests == {MetaManifest("", "a", "", "b")}

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
