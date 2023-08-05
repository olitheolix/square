from typing import Tuple

import square.dtypes


def modify_patch_manifests(square_config: "square.dtypes.Config",
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
    assert isinstance(square_config, square.dtypes.Config)
    return local_manifest, server_manifest
