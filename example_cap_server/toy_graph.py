from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Edge:
    source: str
    target: str
    weight: float


NODE_BASELINES = {
    "marketing_spend": 10.0,
    "product_quality": 7.0,
    "demand": 42.0,
    "retention": 0.8,
    "revenue": 100.0,
}

NODE_TYPES = {
    "marketing_spend": "input",
    "product_quality": "input",
    "demand": "state",
    "retention": "state",
    "revenue": "outcome",
}

NODE_DOMAINS = {
    "marketing_spend": "go_to_market",
    "product_quality": "product",
    "demand": "commercial",
    "retention": "commercial",
    "revenue": "finance",
}

EDGES = (
    Edge("marketing_spend", "demand", 0.6),
    Edge("product_quality", "demand", 1.2),
    Edge("product_quality", "retention", 0.05),
    Edge("demand", "revenue", 2.0),
    Edge("retention", "revenue", 12.0),
)

GRAPH_ID = "toy-business-dag"
GRAPH_VERSION = "2026-03-demo"
GRAPH_DESCRIPTION = (
    "Synthetic business graph used only to demonstrate CAP discovery, invocation, "
    "and semantic disclosure over a small directed acyclic graph."
)


def list_nodes() -> list[str]:
    return list(NODE_BASELINES.keys())


def parent_edges(node_id: str) -> list[Edge]:
    return [edge for edge in EDGES if edge.target == node_id]


def child_edges(node_id: str) -> list[Edge]:
    return [edge for edge in EDGES if edge.source == node_id]


def neighbors(node_id: str, scope: str) -> list[str]:
    if scope == "parents":
        return [edge.source for edge in parent_edges(node_id)]
    if scope == "children":
        return [edge.target for edge in child_edges(node_id)]
    raise ValueError(f"Unsupported scope: {scope}")


def markov_blanket(node_id: str) -> list[str]:
    parents = set(neighbors(node_id, "parents"))
    children = set(neighbors(node_id, "children"))
    spouses: set[str] = set()
    for child in children:
        spouses.update(neighbors(child, "parents"))
    spouses.discard(node_id)
    blanket = parents | children | spouses
    return sorted(blanket)


def compute_prediction(node_id: str) -> float:
    baseline = NODE_BASELINES[node_id]
    inbound = parent_edges(node_id)
    if not inbound:
        return baseline
    return round(
        baseline + sum(edge.weight * compute_prediction(edge.source) for edge in inbound),
        4,
    )


def strongest_drivers(node_id: str, limit: int = 3) -> list[str]:
    ranked = sorted(
        parent_edges(node_id),
        key=lambda edge: abs(edge.weight),
        reverse=True,
    )
    return [edge.source for edge in ranked[:limit]]


def total_path_effect(source: str, target: str) -> float:
    return round(_path_effects(source, target, visited={source}), 4)


def _path_effects(source: str, target: str, visited: set[str]) -> float:
    if source == target:
        return 1.0

    total = 0.0
    for edge in child_edges(source):
        if edge.target in visited:
            continue
        downstream = _path_effects(edge.target, target, visited | {edge.target})
        if downstream:
            total += edge.weight * downstream
    return total


def find_paths(source: str, target: str, max_paths: int) -> list[list[str]]:
    results: list[list[str]] = []

    def _walk(current: str, trail: list[str]) -> None:
        if len(results) >= max_paths:
            return
        if current == target:
            results.append(trail.copy())
            return
        for edge in child_edges(current):
            if edge.target in trail:
                continue
            _walk(edge.target, trail + [edge.target])

    _walk(source, [source])
    return results
