import importlib.resources
import logging
import multiprocessing
import os
import sys
from pathlib import Path

from . import square
from .cfgfile import load

__version__ = "2.6.0"

logit = logging.getLogger("square")


def _configure_start_method() -> None:
    """Force the `fork` multiprocessing start method on Linux.

    Python 3.14 made `forkserver` the default on Linux, which triggers errors
    deep inside the multiprocessing module for Square, so we switch back to
    `fork` (overridable via `MULTIPROCESSING_START_METHOD`).

    Use `force=True` so this still works when a start method was already set (eg
    by an embedding application, which would otherwise raise `RuntimeError`), and
    ignore an invalid override value rather than letting `import square` crash
    with a `ValueError`.
    """
    if not sys.platform.startswith("linux"):
        return

    mp_method = os.environ.get("MULTIPROCESSING_START_METHOD", "fork")
    try:
        multiprocessing.set_start_method(mp_method, force=True)
    except ValueError:
        logit.warning(f"Ignoring invalid multiprocessing start method <{mp_method}>")


_configure_start_method()

# ---------------------------------------------------------------------------
# Global Runtime Constants
# ---------------------------------------------------------------------------
# Determine the base folder. This is usually the folder of this very file but
# will be different if we run inside a bundle produced by PyInstaller.
if getattr(sys, "_MEIPASS", None):
    BASE_DIR = Path(getattr(sys, "_MEIPASS"))
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
