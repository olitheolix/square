import logging
from typing import Tuple

import square
from square.dtypes import Config, FiltersKind

# Convenience: global logger instance to avoid repetitive code.
logit = logging.getLogger("square")


def modify_patch_manifests(square_config: "Config",
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
    assert isinstance(square_config, Config)
    return local_manifest, server_manifest


def cleanup_manifest(square_config: Config, manifest: dict) -> dict:
    def _update(filters: FiltersKind, manifest: dict):
        """Recursively traverse the `manifest` and prune it according to `filters`.

        Returns the input `manifest` but with the excluded sections.

        Raise `KeyError` if an invalid key was found.

        """
        # Split the list of strings and dicts into a dedicated set of string
        # and dedicated list of dicts.
        # Example: ["foo", "bar", {"a": "b", "c": "d"}] will become
        #   {"foo", "bar"} and {"a": "b", "c", "d"}.
        filter_str = {_ for _ in filters if isinstance(_, str)}
        filter_map = [_ for _ in filters if isinstance(_, dict)]
        filter_map = {k: v for d in filter_map for k, v in d.items()}

        # Iterate over the manifest. Prune all keys that match the `filters`
        # and record them in `removed`.
        removed = {}
        for k, v in list(manifest.items()):
            if k in filter_str:
                # Remove the entire key (and all sub-fields if present).
                # NOTE: it does not matter if the key also exists in
                # `filter_map` - we remove the entire key.
                logit.debug(f"Remove <{k}>")
                removed[k] = manifest.pop(k)
            elif isinstance(v, list) and k in filter_map:
                # Recursively filter each list element.
                tmp = [_update(filter_map[k], _) for _ in v]
                removed[k] = [_ for _ in tmp if _]

                # Do not leave empty elements in the list.
                manifest[k] = [_ for _ in v if _]
            elif isinstance(v, dict) and k in filter_map:
                # Recursively filter each dictionary element.
                logit.debug(f"Dive into <{k}>")
                removed[k] = _update(filter_map[k], manifest[k])
            else:
                logit.debug(f"Skip <{k}>")

            # Remove the key from the manifest altogether if it has become empty.
            if not manifest.get(k, "non-empty"):
                del manifest[k]

        # Remove all empty sub-dictionaries from `removed`.
        return {k: v for k, v in removed.items() if v != {}}

    # Look up the filters for the current resource in the following order:
    # 1) Supplied `manifest_filters`
    # 2) Square default filters for this resource `kind`.
    # Pick the first one that matches.
    kind = manifest["kind"]

    default_filter = square.DEFAULT_CONFIG.filters["_common_"]
    filters: FiltersKind = square_config.filters.get(kind, default_filter)

    # Remove the keys from the `manifest` according to `filters`.
    _update(filters, manifest)
    return manifest
