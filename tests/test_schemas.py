import square.schemas


class TestMainGet:
    def test_schemas_default(self):
        """Manually verify one of the schemas that it has the default values."""
        for data in square.schemas.EXCLUSION_SCHEMA.values():
            assert data["metadata"]["uid"] is False

    def test_populate_schemas(self):
        exclude = {"TEST": {"metadata": {"labels": "neither-bool-nor-dict"}}}
        assert square.schemas.populate_schemas(exclude) is True
