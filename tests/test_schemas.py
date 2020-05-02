import square.schemas


class TestMainGet:
    def test_sane_filter(self):
        # Must be list.
        assert square.schemas.valid([]) is True
        assert square.schemas.valid({}) is False

        # List must not contain empty strings.
        assert square.schemas.valid([""]) is False
        assert square.schemas.valid(["foo"]) is True

        # Dictionaries must have exactly one key.
        assert square.schemas.valid([{}]) is False
        assert square.schemas.valid([{"foo": "foo"}]) is False
        assert square.schemas.valid([{"foo": ["foo"]}]) is True
        assert square.schemas.valid([{"foo": "foo", "bar": "bar"}]) is False

        # List must only contain dictionaries and strings.
        assert square.schemas.valid(["foo"]) is True
        assert square.schemas.valid(["foo", {"bar": ["y"]}]) is True
        assert square.schemas.valid(["foo", None]) is False

        # Nested cases:
        assert square.schemas.valid(["foo", {"bar": ["bar"]}]) is True
        assert square.schemas.valid(["foo", {"bar": [{"x": ["x"]}]}]) is True
        assert square.schemas.valid(["foo", {"bar": [{"x": "x"}]}]) is False

    def test_default_filters(self):
        assert square.schemas.default() == [
            {"metadata": [
                {"annotations": [
                    "deployment.kubernetes.io/revision",
                    "kubectl.kubernetes.io/last-applied-configuration",
                    "kubernetes.io/change-cause",
                ]},
                "creationTimestamp",
                "generation",
                "resourceVersion",
                "selfLink",
                "uid",
            ]},
            "status",
        ]

    def test_merge(self):
        defaults = square.schemas.default()
        assert square.schemas.merge(defaults, []) == defaults

        expected = [
            # Default schema.
            {"foo": ["bar"]},
            {"metadata": [
                {"annotations": [
                    "deployment.kubernetes.io/revision",
                    "kubectl.kubernetes.io/last-applied-configuration",
                    "kubernetes.io/change-cause",
                ]},
                "creationTimestamp",
                "generation",
                "resourceVersion",
                "selfLink",
                "uid",
            ]},
            "status",
        ]
        src = [{"foo": ["bar"]}]
        assert square.schemas.merge(defaults, src) == expected

        expected = [
            {"metadata": [
                {"annotations": [
                    "deployment.kubernetes.io/revision",
                    "kubectl.kubernetes.io/last-applied-configuration",
                    "kubernetes.io/change-cause",
                    "orig-annot",
                ]},
                "creationTimestamp",
                "generation",
                "orig-meta",
                "resourceVersion",
                "selfLink",
                "uid",
            ]},
            "status",
        ]
        src = [
            {"metadata": [
                {"annotations": ["orig-annot"]},
                "orig-meta",
            ]},
        ]
        assert square.schemas.merge(defaults, src) == expected

        expected = [
            {"metadata": [
                {"annotations": [
                    "deployment.kubernetes.io/revision",
                    "kubectl.kubernetes.io/last-applied-configuration",
                    "kubernetes.io/change-cause",
                ]},
                "creationTimestamp",
                "generation",
                "orig-meta",
                "resourceVersion",
                "selfLink",
                "uid",
            ]},
            "status",
        ]
        src = [
            {"metadata": ["orig-meta"]},
        ]
        assert square.schemas.merge(defaults, src) == expected
