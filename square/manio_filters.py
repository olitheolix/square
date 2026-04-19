from jsonpath_ng.exceptions import JSONPathError
import copy
from typing import Any, Dict, List, Tuple

import jsonpath_ng as jp


def remove_empty_dicts(data: Any) -> Any:
    """Recursively remove keys with empty dict values from dicts.

    - If `data` is a dict, recursively process all values and then remove any
      keys whose processed value is an empty dict.
    - If `data` is a list, recursively process each element but never remove
      elements (even if they are empty dicts).
    - For any other type, return `data` unchanged.

    Inputs:
        data: Any

    Returns:
        Any: the cleaned data structure.

    """
    if isinstance(data, dict):
        cleaned = {}
        for k, v in data.items():
            processed = remove_empty_dicts(v)
            if processed == {}:
                # Drop keys whose value is (or became) an empty dict.
                continue
            cleaned[k] = processed
        return cleaned
    elif isinstance(data, list):
        return [remove_empty_dicts(item) for item in data]
    else:
        return data


def _delete_match(jpmatch: jp.DatumInContext) -> None:
    """Delete a single jsonpath-ng match from the document in-place.

    jsonpath_ng provides `match.context` (the parent object) and
    `match.path` (the final step), which we use to delete the matched value.

    """
    assert jpmatch.context

    parent = jpmatch.context.value
    path = jpmatch.path
    assert isinstance(path, (jp.Index, jp.Fields))

    # The `idx` attribute is either a list of array indices or a list of dict
    # keys, depending on the type. Either way, we can use Python's `del`
    # statement for both.
    idx = path.indices if isinstance(path, jp.Index) else path.fields
    for i in idx:
        del parent[i]


def strip_manifest_paths(
    manifest: Dict[str, Any], paths_to_remove: List[str]
) -> Tuple[Dict[str, Any], bool]:
    """
    Remove specified JSONPath paths from a Kubernetes manifest.

    Returns a (modified_copy, error) tuple. If any path is malformed, returns
    the unmodified copy and error=True.

    Path syntax examples:
        "metadata.labels"
        "spec.containers[0].image"
        "spec.containers[*].env"
        "metadata.labels['kubernetes.io/hostname']"

    """
    manifest = copy.deepcopy(manifest)

    # Fail fast: parse and validate all expressions before we mutate the result.
    paths: List[jp.JSONPath]
    try:
        paths = [jp.parse(path) for path in paths_to_remove]
    except JSONPathError:
        return manifest, True

    # Collect all matches across all expressions, then sort by index descending
    # so that deleting array elements by index doesn't shift subsequent indices.
    all_matches: List[jp.DatumInContext] = []
    for path in paths:
        all_matches.extend(path.find(manifest))
    all_matches.sort(key=lambda m: str(m.full_path), reverse=True)

    for jpmatch in all_matches:
        _delete_match(jpmatch)

    manifest = remove_empty_dicts(manifest)
    return manifest, False
