import square.schemas


class TestMainGet:
    def test_populate_schemas(self):
        exclude = {"TEST": {"metadata": {"labels": "neither-bool-nor-dict"}}}
        assert square.schemas.populate_schemas(exclude) is True
