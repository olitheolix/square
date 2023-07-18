"""Setup YAML to use fast loaders if possible, and fold multi-line strings.

This file does not really contain any logic as such. Its sole purpose is setup
the YAML library such that it uses the fast CSafeLoader/CSafeDumper from the
LibYAML C library if they are available on the host, or fall back to the slow
Python loader/dumper if not.

Furthermore, it will configure the Dumper to use the "|" syntax for multi-line
strings. This makes ConfigMaps in particular more readable.

To make use of this, the other modules can simply use:

   from yaml_io import Loader, Dumper

"""
import logging

# Convenience: global logger instance to avoid repetitive code.
logit = logging.getLogger("square")

# ----------------------------------------------------------------------------
# NOTE: this import is excluded from the coverage report since it is difficult
#       to write a meaningful test around a library import that may or may not
#       exist on the host. I deem this acceptable in this case because it is a
#       widely used snippet and devoid of logic.
# ----------------------------------------------------------------------------
try:                                 # codecov-skip
    from yaml import (  # type: ignore
        CSafeDumper as Dumper, CSafeLoader as Loader,
    )
    logit.debug("Using LibYAML C library")
except ImportError:                  # codecov-skip
    from yaml import Dumper, Loader  # type: ignore
    logit.debug("Using Python YAML library")


def fold_yaml_strings(dumper, data):
    """
    Fold all YAML strings that contain a new-line, ie use the `|` notation, eg

    foo: |
      this is
      a multi-line string.

    """
    if '\n' in data:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


# Install the new string converter into the Dumper.
Dumper.add_representer(str, fold_yaml_strings)

# We must now have a `Dumper` and `Loader` that other modules can import.
assert Loader is not None
assert Dumper is not None
