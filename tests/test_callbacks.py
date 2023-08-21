import copy
from typing import Any, Dict

import square
import square.callbacks
import square.callbacks as callbacks
from square.dtypes import Config, FiltersKind


class TestCallbacks:
    def test_modify_patch_manifests(self, config: Config):
        """The default patch callback must do notingh."""
        local_manifest = {"local": "manfiest"}
        server_manifest = {"server": "manfiest"}

        # Default callback must return the inputs verbatim.
        ret = square.callbacks.modify_patch_manifests(
            config,
            local_manifest,
            server_manifest,
        )
        assert ret == (local_manifest, server_manifest)


class TestManifestFiltering:
    def test_strip_manifest_generic(self, config):
        """Create a completely fake filter set to test all options.

        This test has nothing to do with real world manifests. Its only purpose
        is to validate the algorithm that strips them.

        """
        # Define filters for this test.
        _filters: FiltersKind = [
            "invalid",
            {"metadata": [
                "creationTimestamp",
                {"labels": ["foo"]},
                "foo",
            ]},
            "status",
            {"spec": [{"ports": ["nodePort"]}]},
        ]
        config.filters = {"Service": _filters}

        # Demo manifest. None of its keys matches the filter and
        # `strip` must therefore not remove anything.
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "name", "namespace": "ns"},
            "spec": "spec",
        }
        assert callbacks.strip_manifest(config, manifest) == manifest
        del manifest

        # Demo manifest. The "labels.foo" matches the filter and must not survive.
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "name",
                "namespace": "ns",
                "labels": {"foo": "remove"}
            },
            "spec": "spec",
        }
        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "name",
                "namespace": "ns",
            },
            "spec": "spec",
        }
        assert callbacks.strip_manifest(config, manifest) == expected
        del manifest

        # Valid manifest with all mandatory and *some* optional keys (
        # "metadata.labels" and "optional-2" are present, "optional-1" is
        # not).
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "mandatory",
                "namespace": "ns",
                "labels": {
                    "foo": "remove",
                    "bar": "keep",
                }
            },
            "spec": "keep",
            "status": "remove",
        }
        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "mandatory",
                "namespace": "ns",
                "labels": {
                    "bar": "keep",
                }
            },
            "spec": "keep",
        }
        assert callbacks.strip_manifest(config, manifest) == expected

    def test_strip_manifest_ambigous_filters(self, config):
        """Must cope with filters that specify the same resource multiple times."""
        # Define filters for this test.
        _filters: FiltersKind = [
            {"metadata": [
                # Specify "creationTimestamp" twice. Must behave no different
                # than if it was specified only once.
                "creationTimestamp",
                "creationTimestamp",
            ]},

            # Reference "status" twice. This must remove the entire "status" field.
            {"status": [{"foo": ["bar"]}]},
            "status",
        ]
        config.filters = {"Service": _filters}

        # Demo manifest. None of its keys matches the filter and
        # `strip` must therefore not remove anything.
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "name", "namespace": "ns"},
            "spec": "spec",
        }
        assert callbacks.strip_manifest(config, manifest) == manifest
        del manifest

        # Demo manifest. The "labels.creationTimestamp" matches the filter and
        # must not survive.
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "name",
                "namespace": "ns",
                "creationTimestamp": "123",
            },
            "spec": "spec",
        }
        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "name",
                "namespace": "ns",
            },
            "spec": "spec",
        }
        assert callbacks.strip_manifest(config, manifest) == expected
        del manifest

        # Valid manifest with a "status" field that must not survive.
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "mandatory",
                "namespace": "ns",
            },
            "spec": "keep",
            "status": "remove",
        }
        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "mandatory",
                "namespace": "ns",
            },
            "spec": "keep",
        }
        assert callbacks.strip_manifest(config, manifest) == expected

    def test_strip_manifest_sub_hierarchies(self, config):
        """Remove an entire sub-tree from the manifest."""
        # Remove the "status" key, irrespective of whether it is a string, dict
        # or list in the actual manifest.
        config.filters = {"Service": ["status"]}

        # Minimally valid manifest.
        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "mandatory", "namespace": "ns"},
        }
        manifest: dict = copy.deepcopy(expected)

        manifest["status"] = "string"
        assert callbacks.strip_manifest(config, manifest) == expected

        manifest["status"] = None
        assert callbacks.strip_manifest(config, manifest) == expected

        manifest["status"] = ["foo", "bar"]
        assert callbacks.strip_manifest(config, manifest) == expected

        manifest["status"] = {"foo", "bar"}
        assert callbacks.strip_manifest(config, manifest) == expected

    def test_strip_manifest_lists_simple(self, config):
        """Filter the `NodePort` key from a list of dicts."""
        # Filter the "nodePort" element from the port list.
        config.filters = {"Service": [{"spec": [{"ports": ["nodePort"]}]}]}

        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "name", "namespace": "ns"},
            "spec": {"type": "NodePort"},
        }
        manifest: dict = copy.deepcopy(expected)
        manifest["spec"]["ports"] = [
            {"nodePort": 1},
            {"nodePort": 3},
        ]
        assert callbacks.strip_manifest(config, manifest) == expected

    def test_strip_manifest_lists_service(self, config):
        """Filter the `NodePort` key from a list of dicts."""
        # Filter the "nodePort" element from the port list.
        config.filters = {"Service": [{"spec": [{"ports": ["nodePort"]}]}]}

        expected: dict = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "name", "namespace": "ns"},
            "spec": {"type": "NodePort"},
        }
        manifest: dict = copy.deepcopy(expected)
        manifest["spec"]["ports"] = [
            {"name": "http", "port": 81, "nodePort": 1},
            {"name": "http", "port": 82},
            {"name": "http", "port": 83, "nodePort": 3},
        ]
        expected["spec"]["ports"] = [
            {"name": "http", "port": 81},
            {"name": "http", "port": 82},
            {"name": "http", "port": 83},
        ]
        assert callbacks.strip_manifest(config, manifest) == expected

    def test_strip_manifest_default_filters(self, config):
        """Must fall back to default filters unless otherwise specified.

        Here we expect the function to strip out the `metadata.uid` because it
        is part of the default filters (see `square/resources/defaultconfig.yaml`).

        """
        manifest: Dict[str, Any] = {
            "apiVersion": "v1",
            "kind": "Service",
            "something": "here",
        }
        expected = manifest.copy()

        manifest["metadata"] = {"name": "name", "namespace": "ns", "uid": "some-uid"}
        expected["metadata"] = {"name": "name", "namespace": "ns"}

        # Must remove the `metadata.uid` field.
        assert callbacks.strip_manifest(config, manifest)
