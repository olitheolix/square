import copy
from typing import cast


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


def make(data: dict):
    """Return `data` as a `DotDict`.

    This function will recursively replace all dictionary. It will also replace
    all tuples by lists.

    The intended input `data` of this function is any valid JSON structure.

    """
    return cast(DotDict, _make(data))


def _make(data):
    """Recursively replace all Python dicts with `DotDict` instances."""
    if not isinstance(data, (list, tuple, dict)):
        return data

    # Recursively convert all elements in lists and dicts.
    if isinstance(data, (list, tuple)):
        return [_make(_) for _ in data]
    else:
        return DotDict({k: _make(v) for k, v in data.items()})


def undo(data: DotDict | dict) -> dict:
    """Remove all `DotDict` instances from `data`.

    This function will recursively replace all `DotDict` instances with their
    plain Python equivalent.

    """
    # Use an explicit cast because the `_undo` helper function is recursive and
    # can thus, in theory, return something other than a dict. However, in
    # practice it cannot because we already know that `data` is a dict and
    # therefore so must be the return value.
    return cast(dict, _undo(data))


def _undo(data):
    """Recursively replace all `DotDict` instances with their Python equivalent."""
    if not isinstance(data, (list, tuple, dict)):
        return data

    # Recursively convert all elements in lists and dicts.
    if isinstance(data, (list, tuple)):
        return [_undo(_) for _ in data]
    else:
        return dict({k: _undo(v) for k, v in data.items()})
