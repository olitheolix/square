import pathlib
import sys

from . import square

__version__ = '0.23.3'


# ---------------------------------------------------------------------------
# Global Runtime Constants
# ---------------------------------------------------------------------------
# Determine the base folder. This is usually the folder of this very file, but
# will be different if we run inside a bundle produced by PyInstaller.
if getattr(sys, '_MEIPASS', None):
    BASE_DIR = pathlib.Path(getattr(sys, '_MEIPASS'))
else:
    BASE_DIR = pathlib.Path(__file__).parent.parent

# Square will source all its default values from this configuration file.
DEFAULT_CONFIG_FILE = BASE_DIR / "resources/defaultconfig.yaml"


# ---------------------------------------------------------------------------
# Expose the primary API of Square for convenience.
# ---------------------------------------------------------------------------
get = square.get_resources
plan = square.make_plan
apply_plan = square.apply_plan
show_plan = square.show_plan
