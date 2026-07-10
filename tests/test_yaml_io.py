import importlib

import pytest
import yaml

import square.yaml_io


class TestYamlIo:
    def test_fallback_loader_is_safe(self):
        """The non-LibYAML fallback must import a *safe* loader.

        On a host without the LibYAML C extension the module falls back to
        `from yaml import Dumper, Loader`. Those plain loaders execute Python
        tags such as `!!python/object/apply:os.system [...]`, i.e. arbitrary
        code execution during `yaml.load`. The fallback must therefore import
        the Safe variants, which reject any `!!python/...` tag.

        """
        # Hide the C loaders so reloading the module takes the fallback branch.
        saved = {}
        if hasattr(yaml, "CSafeLoader"):
            saved["CSafeLoader"] = yaml.CSafeLoader
            saved["CSafeDumper"] = yaml.CSafeDumper
            del yaml.CSafeLoader
            del yaml.CSafeDumper
        try:
            importlib.reload(square.yaml_io)

            # A safe loader rejects Python-specific tags; the unsafe loader
            # would happily construct one (and could just as easily run code).
            with pytest.raises(yaml.YAMLError):
                yaml.load("!!python/tuple [1, 2]", Loader=square.yaml_io.Loader)
        finally:
            # Restore the C loaders and reload so the rest of the suite is
            # unaffected by our tampering.
            for name, cls in saved.items():
                setattr(yaml, name, cls)
            importlib.reload(square.yaml_io)
