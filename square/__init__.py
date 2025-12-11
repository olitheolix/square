import importlib.resources
import multiprocessing
import os
import sys
from pathlib import Path

from . import square
from .cfgfile import load

__version__ = '2.2.0'

# This is a workaround for a multiprocessing problem on Linux that started with
# Python 3.14. In that version, Python made the `forkserver` the default method
# to create new processes. Unfortunately, that leads to errors deep inside the
# multiprocessing module. Therefore, we switch back to the `fork` method from
# previous versions unless explicitly overridden.
if sys.platform.startswith("linux"):
    mp_method = os.environ.get("MULTIPROCESSING_START_METHOD", "fork")
    multiprocessing.set_start_method(mp_method)

# ---------------------------------------------------------------------------
# Global Runtime Constants
# ---------------------------------------------------------------------------
# Determine the base folder. This is usually the folder of this very file but
# will be different if we run inside a bundle produced by PyInstaller.
if getattr(sys, '_MEIPASS', None):
    BASE_DIR = Path(getattr(sys, '_MEIPASS'))
    DEFAULT_CONFIG_FILE = BASE_DIR / "resources" / "defaultconfig.yaml"
else:
    with importlib.resources.path("square.resources", "defaultconfig.yaml") as f:
        DEFAULT_CONFIG_FILE = f

# Square will source all its default values from this configuration file. The
# only exceptions are the namespaces. By default, Square will target all the
# namespaces whereas the specimen only declares the "default". The user
# can override this via line arguments or a dedicated ".square.yaml".
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
