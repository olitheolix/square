"""Templates for user callbacks.

Square uses these callbacks by default but users can supply their in the
`Config` structure.

The `strip_manifest` and `patch_manifest` serve different purposes. The
`strip_manifest` callback determines the content of the local manifests
when they get imported. This is useful to exclude, for instance the `.status`
and `.metadata.uid` fields in the locally saved manifests.

The `patch_manifest`, on the other hand offers the means to propagate only
specific changes to a cluster, eg patch the labels and ignore all other
differences between local and cluster manifests.

"""

import logging
from typing import Tuple

import square.manio
from square.dtypes import Config

# Convenience: global logger instance to avoid repetitive code.
logit = logging.getLogger("square")


def patch_manifests(config: Config,
                    local_manifest: dict,
                    server_manifest: dict) -> Tuple[dict, dict]:
    """Return a possibly modified version of local and server manifest.

    Square will call this function for every resource that may require a patch
    but *before* it actually computes the patch.

    The function must return two valid manifests but is otherwise free to alter
    them. The intended use case is to exclude certain parts of the manifests
    from the patch. For instance, the function could set the `.spec.replicas`
    field in both `{local,server}_manifest` to exclude it from the patch.

    If the two returned manifests are identical then Square will not patch the
    resource because they are, well, identical. This is an easy way to
    selectively exclude resources from patches.

    It is safe to modify the input dict, ie there is no need to create a
    (deep)copy of `{local,server}_manifests` first.

    IMPORTANT: only modify the `local_manifest` and return `server_manifest` as
    is, unless you know what you are doing. Changes in the `server_manifest`
    may render the final patch incompatible with the resource in Kubernetes. It
    may even have unintended side effects. You have been warned.

    """
    assert isinstance(config, Config)
    return local_manifest, server_manifest


def strip_manifest(config: Config, manifest: dict) -> dict:
    """Return a possibly modified version of the manifest.

    Square will call this function for every local and server manifest when it
    creates a `plan`, as well as for every server (but not local) manifest when
    it imports resources with `get_resources`.

    The function must return a valid manifest but is otherwise free to alter it
    as necessary. The intended use case is to strip out the portions of the
    manifest that should not make it into the local manifest corpus. The
    `.status` field is a typical example.

    """
    return square.manio.strip_manifest(config, manifest)
