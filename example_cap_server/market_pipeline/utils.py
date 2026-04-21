from __future__ import annotations

from typing import Any

from example_cap_server import toy_graph


def dedupe_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def is_known_node(node_id: str) -> bool:
    return node_id in toy_graph.NODE_BASELINES


def missing_nodes_result(*node_ids: str) -> dict[str, Any]:
    unique = dedupe_preserve(list(node_ids))
    return {
        "status": "missing_nodes",
        "missing_nodes": unique,
        "message": "Some nodes are not present in the currently mounted graph.",
    }


def require_node_param(params: dict[str, Any], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"`params.{key}` is required and must be a string.")
    return value


def coerce_int(value: Any, *, default: int, minimum: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, parsed)


def coerce_float(value: Any, *, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
