from square.manio_filters import strip_manifest_paths, remove_empty_dicts

# ---------------------------------------------------------------------------
# Tests for remove_empty_dicts
# ---------------------------------------------------------------------------


class TestRemoveEmptyDicts:
    def test_non_dict_types_unchanged(self):
        """Non-dict, non-list types must be returned unchanged."""
        assert remove_empty_dicts(42) == 42
        assert remove_empty_dicts("hello") == "hello"
        assert remove_empty_dicts(None) is None
        assert remove_empty_dicts(3.14) == 3.14
        assert remove_empty_dicts(True) is True

    def test_empty_dict_returns_empty_dict(self):
        """An empty dict must remain an empty dict."""
        assert remove_empty_dicts({}) == {}

    def test_flat_dict_no_empty_values(self):
        """A flat dict with no empty-dict values must be returned unchanged."""
        data = {"a": 1, "b": "hello", "c": [1, 2, 3]}
        assert remove_empty_dicts(data) == data

    def test_multiple_empty_dict_values(self):
        """Multiple keys with empty dict values must all be removed."""
        data = {"a": {}, "b": {}, "c": 42}
        assert remove_empty_dicts(data) == {"c": 42}

    def test_all_empty_dict_values(self):
        """If all values are empty dicts the result must be an empty dict."""
        data = {"a": {}, "b": {}}
        assert remove_empty_dicts(data) == {}

    def test_nested_dict_becomes_empty(self):
        """A nested dict that becomes empty after stripping must itself be removed."""
        data = {"outer": {"inner": {}}}
        assert remove_empty_dicts(data) == {}

    def test_nested_dict_partially_empty(self):
        """Only the empty sub-keys must be removed; non-empty ones must remain."""
        data = {
            "metadata": {
                "name": "test",
                "labels": {},
            }
        }
        assert remove_empty_dicts(data) == {"metadata": {"name": "test"}}

    def test_deeply_nested_with_non_empty_sibling(self):
        """Deeply nested empty dicts are removed but non-empty siblings remain."""
        data = {
            "a": {
                "b": {"c": {}},
                "d": 42,
            }
        }
        assert remove_empty_dicts(data) == {"a": {"d": 42}}

    def test_list_elements_not_removed(self):
        """Empty dicts that are list elements must NOT be removed from the list."""
        data = [1, {}, "hello", {}]
        assert remove_empty_dicts(data) == [1, {}, "hello", {}]

    def test_list_of_dicts_stripped_internally(self):
        """Dicts inside a list must have their empty-dict keys stripped."""
        data = [
            {"a": 1, "b": {}},
            {"c": {}, "d": "hello"},
        ]
        assert remove_empty_dicts(data) == [
            {"a": 1},
            {"d": "hello"},
        ]

    def test_list_of_dicts_becoming_empty(self):
        """Dicts inside a list that become empty after stripping remain as empty dicts."""
        data = [{"a": {}}, {"b": {}}]
        assert remove_empty_dicts(data) == [{}, {}]

    def test_dict_with_list_of_dicts(self):
        """Dicts containing lists of dicts must be handled correctly."""
        data = {
            "spec": {
                "containers": [
                    {"name": "nginx", "env": {}},
                    {"name": "redis", "resources": {}},
                ]
            }
        }
        assert remove_empty_dicts(data) == {
            "spec": {
                "containers": [
                    {"name": "nginx"},
                    {"name": "redis"},
                ]
            }
        }

    def test_empty_list_not_removed(self):
        """An empty list must not be removed (only empty dicts are removed)."""
        data = {"a": [], "b": 1}
        assert remove_empty_dicts(data) == {"a": [], "b": 1}

    def test_nested_lists(self):
        """Nested lists must be handled recursively."""
        data = [[{"a": {}}, {"b": 1}], [{}]]
        assert remove_empty_dicts(data) == [[{}, {"b": 1}], [{}]]


# ---------------------------------------------------------------------------
# Tests for strip_manifest_paths (pre-existing tests, unchanged)
# ---------------------------------------------------------------------------


def test_basic_stripping():
    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "labels": {"app": "test"}},
        "spec": {"containers": [{"name": "nginx", "image": "nginx:latest"}]},
    }

    result = strip_manifest_paths(manifest, ["metadata.labels"])
    assert result == {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod"},
        "spec": {"containers": [{"name": "nginx", "image": "nginx:latest"}]},
    }


def test_array_index_stripping():
    manifest = {
        "spec": {
            "containers": [
                {"name": "redis", "image": "redis:latest"},
                {"name": "nginx", "image": "nginx:latest"},
                {"name": "redis", "image": "redis:alpine"},
            ]
        }
    }

    result = strip_manifest_paths(manifest, ["spec.containers[1]"])
    assert result == {
        "spec": {
            "containers": [
                {"name": "redis", "image": "redis:latest"},
                {"name": "redis", "image": "redis:alpine"},
            ]
        }
    }


def test_array_index_stripping_multi():
    manifest = {
        "spec": {
            "containers": [
                {"name": "redis", "image": "redis:latest"},
                {"name": "nginx", "image": "nginx:latest"},
                {"name": "redis", "image": "redis:alpine"},
            ]
        }
    }

    paths = ["spec.containers[0]", "spec.containers[2]"]
    result = strip_manifest_paths(manifest, paths)
    assert result == {
        "spec": {"containers": [{"name": "nginx", "image": "nginx:latest"}]}
    }


def test_wildcard_stripping():
    manifest = {
        "spec": {
            "containers": [
                {"name": "nginx", "env": [{"name": "VAR1", "value": "val1"}]},
                {"name": "redis", "env": [{"name": "VAR2", "value": "val2"}]},
            ]
        }
    }

    paths = ["spec.containers[*].env"]
    result = strip_manifest_paths(manifest, paths)
    assert result == {
        "spec": {
            "containers": [
                {"name": "nginx"},
                {"name": "redis"},
            ]
        }
    }


def test_nested_path_stripping():
    manifest = {
        "metadata": {
            "name": "test",
            "annotations": {"key1": "value1", "key2": "value2"},
        }
    }

    paths = ["metadata.annotations.key1"]
    result = strip_manifest_paths(manifest, paths)
    assert result == {
        "metadata": {
            "name": "test",
            "annotations": {"key2": "value2"},
        }
    }


def test_multiple_paths_stripping():
    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test",
            "labels": {"app": "test"},
            "annotations": {"key": "value"},
        },
        "spec": {"containers": [{"name": "nginx"}], "replicas": 3},
    }

    paths = ["metadata.labels", "spec.replicas"]
    result = strip_manifest_paths(manifest, paths)
    assert result == {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test",
            "annotations": {"key": "value"},
        },
        "spec": {"containers": [{"name": "nginx"}]},
    }


def test_multiple_paths_stripping2():
    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test",
            "labels": {"app": "test"},
        },
    }

    paths = ["metadata.labels", "metadata.labels.app"]
    result = strip_manifest_paths(manifest, paths)
    assert result == {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test",
        },
    }


def test_empty_manifest():
    result = strip_manifest_paths({}, ["metadata"])
    assert result == {}


def test_nonexistent_path():
    manifest = {"metadata": {"name": "test"}}
    result = strip_manifest_paths(manifest, ["metadata.labels"])
    assert result == manifest


def test_complex_k8s_manifest():
    manifest = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "nginx-deployment",
            "annotations": {"foo": "bar"},
            "labels": {
                "app": "nginx",
                "kubernetes.io/name": "foo",
                "kubernetes.io/app": "bar",
            },
        },
        "spec": {
            "replicas": 3,
            "selector": {"matchLabels": {"app": "nginx"}},
            "template": {
                "metadata": {"labels": {"app": "nginx"}},
                "spec": {
                    "containers": [
                        {
                            "name": "nginx",
                            "image": "nginx:1.14.2",
                            "ports": [{"containerPort": 80}],
                        }
                    ]
                },
            },
        },
    }

    paths = [
        "metadata.annotations",
        "spec.template.metadata.labels",
        "spec.replicas",
        "metadata.labels['kubernetes.io/name']",
    ]
    result = strip_manifest_paths(manifest, paths)
    assert result == {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "nginx-deployment",
            "labels": {"app": "nginx", "kubernetes.io/app": "bar"},
        },
        "spec": {
            "selector": {"matchLabels": {"app": "nginx"}},
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "nginx",
                            "image": "nginx:1.14.2",
                            "ports": [{"containerPort": 80}],
                        }
                    ]
                },
            },
        },
    }
