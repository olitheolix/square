import copy


class DotDict(dict):
    """Dictionary that supports element access with '.'.

    Obviously, the usual Python rules apply. For instance, if

        dd = {'foo': 0, 'foo bar', 1': 1, 'items': 'xyz}`

    then this is valid:
    * `dd.foo`

    whereas these are not:
    * `dd.0`
    * `dd.foo bar`

    and
    * `dd.items`

    returns the `items` *method* of the underlying `dict`, not `dd["items"]`.

    """
    def __getattr__(self, key):
        return self[key]

    def __deepcopy__(self, *args, **kwargs):
        # To copy a `DotDict`, first convert it to a normal Python dict, then
        # let the `copy` module do its work and afterwards return a `DotDict`
        # version of that copy.
        return make(copy.deepcopy(dict(self)))

    def __copy__(self, *args, **kwargs):
        return self.__deepcopy__(*args, **kwargs)


def make(data):
    """Return `data` as a `DotDict`.

    This function will recursively replace all dictionary. It will also replace
    all tuples by lists.

    The intended input `data` of this function is any valid JSON structure.

    """
    if not isinstance(data, (list, tuple, dict)):
        return data

    # Recursively convert all elements in lists and dicts.
    if isinstance(data, (list, tuple)):
        return [make(_) for _ in data]
    else:
        return DotDict({k: make(v) for k, v in data.items()})


def undo(data):
    """Remove all `DotDict` instances from `data`.

    This function will recursively replace all `DotDict` instances with their
    plain Python equivalent.

    """
    if not isinstance(data, (list, tuple, dict)):
        return data

    # Recursively convert all elements in lists and dicts.
    if isinstance(data, (list, tuple)):
        return [undo(_) for _ in data]
    else:
        return dict({k: undo(v) for k, v in data.items()})
