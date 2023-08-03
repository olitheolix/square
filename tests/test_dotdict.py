import copy

import square.dotdict as dd


class TestBasic:
    def test_make(self):
        """Create various DotDict instances."""
        # Valid dict. Verify Dot access.
        d = dd.make({'foo': 'bar'})
        assert d.foo == 'bar'

        # These are not dictionaries but must return a comparable type nevertheless.
        assert dd._make(None) is None
        assert dd._make(3) == 3
        assert dd._make('3') == '3'
        assert dd._make(['3', 3]) == ['3', 3]
        assert dd._make(('3', 3)) == ['3', 3]

        # A list of dicts.
        d = dd._make([{'foo0': 'bar0'}, {'foo1': 'bar1'}])
        assert d[0].foo0 == 'bar0'
        assert d[1].foo1 == 'bar1'

        # A dict of dicts.
        d = dd.make({'foo0': {'foo1': 'bar1'}})
        assert d.foo0.foo1 == 'bar1'

        # The `make` function must have operated recursively.
        assert isinstance(d, dd.DotDict)
        assert isinstance(d.foo0, dd.DotDict)

    def test_undo(self):
        """Verify that `undo` does just that."""
        # Demo dictionary.
        src = {'foo0': {'foo1': 'bar1'}}
        ddict = dd.make(src)

        # Converting a DotDict back via `dict` is possible but does not act
        # recursively.
        ddict_undo = dict(ddict)
        assert type(ddict_undo) is dict
        assert type(ddict_undo["foo0"]) is not dict  # <- Still a `DotDict`!
        assert src == ddict_undo
        del ddict_undo

        # Exactly the same test, but this time we use the `undo`
        # function which operates recursively and will ensure that the
        # sub-dicts are also converted back to plain Python dicts.
        ddict_undo = dd.undo(ddict)
        assert type(ddict_undo) is dict
        assert type(ddict_undo["foo0"]) is dict  # <- Now a plain `dict`!
        assert src == ddict_undo

    def test_make_deepcopy(self):
        """Verify that it is possible to copy DotDict instances."""
        # Demo dictionary.
        src = {'foo0': {'foo1': 'bar1'}}

        # DotDict must test positive for equality.
        ddict = dd.make(src)
        assert isinstance(ddict, dd.DotDict)
        assert isinstance(ddict.foo0, dd.DotDict)
        assert src == ddict

        # Deepcopy must work.
        ddict2 = copy.deepcopy(ddict)
        ddict3 = copy.deepcopy(ddict2)
        assert src == ddict2 == ddict3
        assert isinstance(ddict, dd.DotDict)
        assert isinstance(ddict2, dd.DotDict)
        assert ddict is not ddict2
        assert ddict is not ddict3

        # Normal copy must work.
        ddict4 = copy.copy(ddict)
        assert src == ddict4
