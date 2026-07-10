import os
import sys
from unittest import mock

import square


class TestConfigureStartMethod:
    def test_invalid_env_value_is_ignored(self):
        """A bad MULTIPROCESSING_START_METHOD must not crash `import square`."""
        with (
            mock.patch.object(sys, "platform", "linux"),
            mock.patch.dict(
                os.environ, {"MULTIPROCESSING_START_METHOD": "bogus"}, clear=True
            ),
            mock.patch.object(
                square.multiprocessing, "set_start_method", side_effect=ValueError
            ) as m_set,
        ):
            # Must swallow the ValueError rather than propagate it.
            square._configure_start_method()

        m_set.assert_called_once_with("bogus", force=True)

    def test_linux_forces_default_fork(self):
        """On Linux the default method is `fork`, applied with `force=True`."""
        with (
            mock.patch.object(sys, "platform", "linux"),
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch.object(square.multiprocessing, "set_start_method") as m_set,
        ):
            square._configure_start_method()

        m_set.assert_called_once_with("fork", force=True)

    def test_non_linux_is_a_noop(self):
        """Off Linux the start method must be left untouched."""
        with (
            mock.patch.object(sys, "platform", "darwin"),
            mock.patch.object(square.multiprocessing, "set_start_method") as m_set,
        ):
            square._configure_start_method()

        m_set.assert_not_called()
