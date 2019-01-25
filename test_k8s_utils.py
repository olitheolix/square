import copy
import k8s_utils as utils


class TestBasic:
    def test_make_dotdict(self):
        """Create various DotDict instances."""

        # These are not dictionaries but must return a comparable type nevertheless.
        assert utils.make_dotdict(None) is None
        assert utils.make_dotdict(3) == 3
        assert utils.make_dotdict('3') == '3'
        assert utils.make_dotdict(['3', 3]) == ['3', 3]
        assert utils.make_dotdict(('3', 3)) == ['3', 3]

        # Valid dict. Verify Dot access.
        d = utils.make_dotdict({'foo': 'bar'})
        assert d.foo == 'bar'

        # A list of dicts.
        d = utils.make_dotdict([{'foo0': 'bar0'}, {'foo1': 'bar1'}])
        assert d[0].foo0 == 'bar0'
        assert d[1].foo1 == 'bar1'

        # A dict of dicts.
        d = utils.make_dotdict({'foo0': {'foo1': 'bar1'}})
        assert d.foo0.foo1 == 'bar1'

        # The `make_dotdict` function must have operated recursively.
        assert isinstance(d, utils.DotDict)
        assert isinstance(d.foo0, utils.DotDict)

    def test_undo_dotdict(self):
        """Verify that `undo_dotdict` does just that."""
        # Demo dictionary.
        src = {'foo0': {'foo1': 'bar1'}}
        ddict = utils.make_dotdict(src)

        # Converting a DotDict back via `dict` is possible but does not act
        # recursively.
        ddict_undo = dict(ddict)
        assert type(ddict_undo) == dict
        assert type(ddict_undo["foo0"]) != dict  # <- Still a `DotDict`!
        assert src == ddict_undo
        del ddict_undo

        # Exactly the same test, but this time we use the `undo_dotdict`
        # function which operates recursively and will ensure that the
        # sub-dicts are also converted back to plain Python dicts.
        ddict_undo = utils.undo_dotdict(ddict)
        assert type(ddict_undo) == dict
        assert type(ddict_undo["foo0"]) == dict  # <- Now a plain `dict`!
        assert src == ddict_undo

    def test_make_dotdict_deepcopy(self):
        """Verify that it is possible to copy DotDict instances."""
        # Demo dictionary.
        src = {'foo0': {'foo1': 'bar1'}}

        # DotDict must test positive for equality.
        ddict = utils.make_dotdict(src)
        assert isinstance(ddict, utils.DotDict)
        assert isinstance(ddict.foo0, utils.DotDict)
        assert src == ddict

        # Deepcopy must work.
        ddict2 = copy.deepcopy(ddict)
        ddict3 = copy.deepcopy(ddict2)
        assert src == ddict2 == ddict3
        assert isinstance(ddict, utils.DotDict)
        assert isinstance(ddict2, utils.DotDict)
        assert ddict is not ddict2
        assert ddict is not ddict3

        # Normal copy must work.
        ddict4 = copy.copy(ddict)
        assert src == ddict4
