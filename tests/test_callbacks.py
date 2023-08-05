import square
import square.callbacks
from square.dtypes import Config


class TestCallbacks:
    def test_modify_patch_manifests(self, config: Config):
        """The default patch callback must do notingh."""
        local_manifest = {"local": "manfiest"}
        server_manifest = {"server": "manfiest"}

        # Default callback must return the inputs verbatim.
        ret = square.callbacks.modify_patch_manifests(
            square_config=config,
            local_manifest=local_manifest,
            server_manifest=server_manifest,
        )
        assert ret == (local_manifest, server_manifest)
