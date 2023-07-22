import pathlib
import sys

from . import square
from .cfgfile import load

__version__ = '1.4.0'


# ---------------------------------------------------------------------------
# Global Runtime Constants
# ---------------------------------------------------------------------------
# Determine the base folder. This is usually the folder of this very file, but
# will be different if we run inside a bundle produced by PyInstaller.
if getattr(sys, '_MEIPASS', None):
    BASE_DIR = pathlib.Path(getattr(sys, '_MEIPASS'))
else:
    BASE_DIR = pathlib.Path(__file__).parent.parent

# Square will source all its default values from this configuration file. The
# only exceptions are the namespaces. By default, Square will target all the
# namespaces whereas the specimen only declares "default" unless the user
# explicitly says otherwise via the command line arguments or ".square.yaml".
DEFAULT_CONFIG_FILE = BASE_DIR / "resources" / "defaultconfig.yaml"
DEFAULT_CONFIG, err = load(DEFAULT_CONFIG_FILE)
DEFAULT_CONFIG.selectors.namespaces.clear()
assert not err

# ---------------------------------------------------------------------------
# Expose the primary API of Square for convenience.
# ---------------------------------------------------------------------------
get = square.get_resources
plan = square.make_plan
apply_plan = square.apply_plan
show_plan = square.show_plan
