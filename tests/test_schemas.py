import square.schemas


class TestMainGet:
    def test_schemas_default(self):
        """Manually verify one of the schemas that it has the default values."""
        for version, schema in square.schemas.EXCLUSION_SCHEMA.items():
            for data in schema.values():
                assert data["metadata"]["uid"] is False

    def test_populate_schemas(self):
        exclude = {"1.10": {"TEST": {
            "metadata": {"labels": "neither-bool-nor-dict"}
        }}}

        assert square.schemas.populate_schemas(exclude) is True
