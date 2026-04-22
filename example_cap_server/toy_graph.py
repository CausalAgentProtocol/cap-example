from __future__ import annotations

from dataclasses import dataclass
import random


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


def dataset_density_metrics() -> dict[str, float | int]:
    node_ids = list_nodes()
    node_count = len(node_ids)
    edge_count = len(EDGES)
    possible_directed_edges = node_count * (node_count - 1) if node_count >= 2 else 0
    missing_directed_edges = max(0, possible_directed_edges - edge_count)
    density = (
        0.0
        if possible_directed_edges == 0
        else round(edge_count / possible_directed_edges, 4)
    )
    sparsity = round(1.0 - density, 4)

    in_degrees = [len(parent_edges(node_id)) for node_id in node_ids]
    out_degrees = [len(child_edges(node_id)) for node_id in node_ids]
    total_degrees = [
        in_degree + out_degree
        for in_degree, out_degree in zip(in_degrees, out_degrees, strict=False)
    ]
    average_degree = round(sum(total_degrees) / node_count, 4) if node_count else 0.0
    average_in_degree = round(sum(in_degrees) / node_count, 4) if node_count else 0.0
    average_out_degree = round(sum(out_degrees) / node_count, 4) if node_count else 0.0

    return {
        "node_count": node_count,
        "edge_count": edge_count,
        "possible_directed_edges": possible_directed_edges,
        "missing_directed_edges": missing_directed_edges,
        "density": density,
        "sparsity": sparsity,
        "average_degree": average_degree,
        "average_in_degree": average_in_degree,
        "average_out_degree": average_out_degree,
        "max_degree": max(total_degrees, default=0),
        "min_degree": min(total_degrees, default=0),
        "max_in_degree": max(in_degrees, default=0),
        "min_in_degree": min(in_degrees, default=0),
        "max_out_degree": max(out_degrees, default=0),
        "min_out_degree": min(out_degrees, default=0),
        "source_node_count": sum(1 for degree in in_degrees if degree == 0),
        "sink_node_count": sum(1 for degree in out_degrees if degree == 0),
        "isolated_node_count": sum(1 for degree in total_degrees if degree == 0),
    }


def connectivity_report(source: str, target: str, *, max_paths: int) -> dict[str, object]:
    bounded = max(1, int(max_paths))
    source_known = source in NODE_BASELINES
    target_known = target in NODE_BASELINES
    missing_nodes = [
        node_id
        for node_id, known in ((source, source_known), (target, target_known))
        if not known
    ]
    if missing_nodes:
        return {
            "source_node_id": source,
            "target_node_id": target,
            "source_known": source_known,
            "target_known": target_known,
            "connected": False,
            "path_count": 0,
            "max_paths": bounded,
            "truncated": False,
            "shortest_path": None,
            "shortest_path_length": None,
            "longest_path": None,
            "longest_path_length": None,
            "all_paths": [],
            "missing_nodes": missing_nodes,
        }

    capped_paths = find_paths(source, target, bounded + 1)
    truncated = len(capped_paths) > bounded
    all_paths = capped_paths[:bounded]
    connected = bool(all_paths)

    if connected:
        shortest_path = min(all_paths, key=len)
        longest_path = max(all_paths, key=len)
        shortest_len = max(0, len(shortest_path) - 1)
        longest_len = max(0, len(longest_path) - 1)
    else:
        shortest_path = None
        longest_path = None
        shortest_len = None
        longest_len = None

    return {
        "source_node_id": source,
        "target_node_id": target,
        "source_known": True,
        "target_known": True,
        "connected": connected,
        "path_count": len(all_paths),
        "max_paths": bounded,
        "truncated": truncated,
        "shortest_path": shortest_path,
        "shortest_path_length": shortest_len,
        "longest_path": longest_path,
        "longest_path_length": longest_len,
        "all_paths": all_paths,
        "missing_nodes": [],
    }


def path_contribution_report(
    source: str,
    target: str,
    *,
    max_paths: int,
) -> dict[str, object]:
    bounded = max(1, int(max_paths))
    source_known = source in NODE_BASELINES
    target_known = target in NODE_BASELINES
    missing_nodes = [
        node_id
        for node_id, known in ((source, source_known), (target, target_known))
        if not known
    ]
    if missing_nodes:
        return {
            "source_node_id": source,
            "target_node_id": target,
            "source_known": source_known,
            "target_known": target_known,
            "connected": False,
            "path_count": 0,
            "max_paths": bounded,
            "truncated": False,
            "total_effect": 0.0,
            "total_absolute_effect": 0.0,
            "top_contributing_path": None,
            "top_contributing_path_effect": None,
            "paths": [],
            "missing_nodes": missing_nodes,
        }

    capped_paths = find_paths(source, target, bounded + 1)
    truncated = len(capped_paths) > bounded
    all_paths = capped_paths[:bounded]
    connected = bool(all_paths)

    raw_rows: list[dict[str, object]] = []
    total_effect = 0.0
    total_abs_effect = 0.0

    for path in all_paths:
        edges = [
            {
                "from_node_id": path[index],
                "to_node_id": path[index + 1],
                "weight": _edge_weight(path[index], path[index + 1]),
            }
            for index in range(len(path) - 1)
        ]
        effect = 1.0
        for edge in edges:
            effect *= float(edge["weight"])
        effect = round(effect, 4)
        abs_effect = round(abs(effect), 4)
        total_effect += effect
        total_abs_effect += abs_effect
        raw_rows.append(
            {
                "node_ids": path,
                "edge_count": max(0, len(path) - 1),
                "edges": edges,
                "path_effect": effect,
                "abs_path_effect": abs_effect,
            }
        )

    total_effect = round(total_effect, 4)
    total_abs_effect = round(total_abs_effect, 4)

    raw_rows = sorted(
        raw_rows,
        key=lambda item: (
            -float(item["abs_path_effect"]),
            str(item["node_ids"]),
        ),
    )
    rows = []
    for index, row in enumerate(raw_rows):
        path_effect = float(row["path_effect"])
        abs_path_effect = float(row["abs_path_effect"])
        rows.append(
            {
                "rank": index + 1,
                **row,
                "share_of_total_effect": (
                    None if total_effect == 0 else round(path_effect / total_effect, 4)
                ),
                "share_of_total_absolute_effect": (
                    None
                    if total_abs_effect == 0
                    else round(abs_path_effect / total_abs_effect, 4)
                ),
            }
        )

    top_row = rows[0] if rows else None
    return {
        "source_node_id": source,
        "target_node_id": target,
        "source_known": True,
        "target_known": True,
        "connected": connected,
        "path_count": len(rows),
        "max_paths": bounded,
        "truncated": truncated,
        "total_effect": total_effect,
        "total_absolute_effect": total_abs_effect,
        "top_contributing_path": None if top_row is None else top_row["node_ids"],
        "top_contributing_path_effect": None if top_row is None else top_row["path_effect"],
        "paths": rows,
        "missing_nodes": [],
    }


def market_impact_report(
    target_node: str,
    intervention_delta: float,
    *,
    min_effect_threshold: float = 0.0001,
) -> dict[str, object]:
    threshold = max(0.0, float(min_effect_threshold))
    target_known = target_node in NODE_BASELINES
    node_count = len(NODE_BASELINES)
    baseline_total = sum(abs(value) for value in NODE_BASELINES.values())

    if not target_known:
        return {
            "target_node": target_node,
            "target_known": False,
            "intervention_delta": round(float(intervention_delta), 4),
            "min_effect_threshold": round(threshold, 6),
            "node_count": node_count,
            "affected_node_count": 0,
            "downstream_affected_node_count": 0,
            "unaffected_node_count": node_count,
            "total_absolute_change": 0.0,
            "average_absolute_change": 0.0,
            "market_change_level": 0.0,
            "max_affected_node": None,
            "max_affected_change": None,
            "max_affected_change_abs": None,
            "affected_nodes": [],
            "missing_nodes": [target_node],
        }

    intervention = float(intervention_delta)
    affected_nodes: list[dict[str, object]] = []
    for node_id in list_nodes():
        effect = total_path_effect(target_node, node_id)
        delta = round(effect * intervention, 4)
        abs_delta = round(abs(delta), 4)
        if abs_delta < threshold:
            continue
        baseline = NODE_BASELINES[node_id]
        relative_change = None if baseline == 0 else round(delta / baseline, 4)
        affected_nodes.append(
            {
                "node_id": node_id,
                "delta": delta,
                "abs_delta": abs_delta,
                "baseline": baseline,
                "relative_change": relative_change,
            }
        )

    affected_nodes = sorted(
        affected_nodes,
        key=lambda item: (-float(item["abs_delta"]), str(item["node_id"])),
    )
    affected_count = len(affected_nodes)
    downstream_count = sum(
        1
        for item in affected_nodes
        if isinstance(item.get("node_id"), str) and item["node_id"] != target_node
    )
    unaffected_count = max(0, node_count - affected_count)
    total_abs_change = round(
        sum(float(item["abs_delta"]) for item in affected_nodes),
        4,
    )
    average_abs_change = round(total_abs_change / node_count, 4) if node_count else 0.0
    market_change_level = (
        0.0
        if baseline_total == 0
        else round(total_abs_change / baseline_total, 4)
    )
    max_item = affected_nodes[0] if affected_nodes else None

    return {
        "target_node": target_node,
        "target_known": True,
        "intervention_delta": round(intervention, 4),
        "min_effect_threshold": round(threshold, 6),
        "node_count": node_count,
        "affected_node_count": affected_count,
        "downstream_affected_node_count": downstream_count,
        "unaffected_node_count": unaffected_count,
        "total_absolute_change": total_abs_change,
        "average_absolute_change": average_abs_change,
        "market_change_level": market_change_level,
        "max_affected_node": None if max_item is None else max_item["node_id"],
        "max_affected_change": None if max_item is None else max_item["delta"],
        "max_affected_change_abs": None if max_item is None else max_item["abs_delta"],
        "affected_nodes": affected_nodes,
        "missing_nodes": [],
    }


def multi_intervention_impact_report(
    interventions: list[dict[str, object]],
    *,
    min_effect_threshold: float = 0.0001,
) -> dict[str, object]:
    threshold = max(0.0, float(min_effect_threshold))
    node_count = len(NODE_BASELINES)
    baseline_total = sum(abs(value) for value in NODE_BASELINES.values())

    normalized_interventions = []
    missing_nodes: list[str] = []
    for item in interventions:
        target_node = str(item.get("target_node", ""))
        intervention_delta = float(item.get("intervention_delta", 1.0))
        known = target_node in NODE_BASELINES
        normalized_interventions.append(
            {
                "target_node": target_node,
                "target_known": known,
                "intervention_delta": round(intervention_delta, 4),
            }
        )
        if not known:
            missing_nodes.append(target_node)

    missing_nodes = _dedupe_preserve(missing_nodes)
    known_interventions = [
        item for item in normalized_interventions if bool(item.get("target_known"))
    ]

    node_delta_by_id = {node_id: 0.0 for node_id in list_nodes()}
    for intervention in known_interventions:
        target_node = str(intervention["target_node"])
        intervention_delta = float(intervention["intervention_delta"])
        for node_id in node_delta_by_id:
            node_delta_by_id[node_id] += total_path_effect(target_node, node_id) * intervention_delta

    affected_nodes: list[dict[str, object]] = []
    for node_id in list_nodes():
        delta = round(node_delta_by_id[node_id], 4)
        abs_delta = round(abs(delta), 4)
        if abs_delta < threshold:
            continue
        baseline = NODE_BASELINES[node_id]
        relative_change = None if baseline == 0 else round(delta / baseline, 4)
        affected_nodes.append(
            {
                "node_id": node_id,
                "delta": delta,
                "abs_delta": abs_delta,
                "baseline": baseline,
                "relative_change": relative_change,
            }
        )

    affected_nodes = sorted(
        affected_nodes,
        key=lambda item: (-float(item["abs_delta"]), str(item["node_id"])),
    )
    affected_count = len(affected_nodes)
    unaffected_count = max(0, node_count - affected_count)
    total_abs_change = round(
        sum(float(item["abs_delta"]) for item in affected_nodes),
        4,
    )
    average_abs_change = round(total_abs_change / node_count, 4) if node_count else 0.0
    market_change_level = (
        0.0
        if baseline_total == 0
        else round(total_abs_change / baseline_total, 4)
    )
    max_item = affected_nodes[0] if affected_nodes else None

    intervention_summaries = []
    for intervention in normalized_interventions:
        target_node = str(intervention["target_node"])
        intervention_delta = float(intervention["intervention_delta"])
        local = market_impact_report(
            target_node,
            intervention_delta,
            min_effect_threshold=threshold,
        )
        intervention_summaries.append(
            {
                "target_node": target_node,
                "target_known": bool(local["target_known"]),
                "intervention_delta": round(intervention_delta, 4),
                "affected_node_count": int(local["affected_node_count"]),
                "market_change_level": float(local["market_change_level"]),
                "total_absolute_change": float(local["total_absolute_change"]),
                "max_affected_node": local["max_affected_node"],
                "max_affected_change_abs": local["max_affected_change_abs"],
            }
        )

    return {
        "intervention_count": len(normalized_interventions),
        "evaluated_intervention_count": len(known_interventions),
        "interventions": normalized_interventions,
        "intervention_summaries": intervention_summaries,
        "min_effect_threshold": round(threshold, 6),
        "node_count": node_count,
        "affected_node_count": affected_count,
        "unaffected_node_count": unaffected_count,
        "total_absolute_change": total_abs_change,
        "average_absolute_change": average_abs_change,
        "market_change_level": market_change_level,
        "max_affected_node": None if max_item is None else max_item["node_id"],
        "max_affected_change": None if max_item is None else max_item["delta"],
        "max_affected_change_abs": None if max_item is None else max_item["abs_delta"],
        "affected_nodes": affected_nodes,
        "missing_nodes": missing_nodes,
    }


def node_systemic_risk_report(
    node_id: str,
    stress_delta: float,
    *,
    min_effect_threshold: float = 0.0001,
) -> dict[str, object]:
    threshold = max(0.0, float(min_effect_threshold))
    node_count = len(NODE_BASELINES)
    node_known = node_id in NODE_BASELINES
    stress = float(stress_delta)

    if not node_known:
        return {
            "node_id": node_id,
            "node_known": False,
            "stress_delta": round(stress, 4),
            "min_effect_threshold": round(threshold, 6),
            "node_count": node_count,
            "in_degree": 0,
            "out_degree": 0,
            "degree": 0,
            "downstream_reachable_count": 0,
            "downstream_reach_ratio": 0.0,
            "affected_node_count": 0,
            "downstream_affected_node_count": 0,
            "unaffected_node_count": node_count,
            "total_absolute_change": 0.0,
            "average_absolute_change": 0.0,
            "market_change_level": 0.0,
            "max_affected_node": None,
            "max_affected_change": None,
            "max_affected_change_abs": None,
            "impact_intensity": 0.0,
            "propagation_breadth": 0.0,
            "concentration_risk": 0.0,
            "structural_centrality": 0.0,
            "systemic_risk_score": 0.0,
            "systemic_risk_level": "unknown",
            "affected_nodes": [],
            "missing_nodes": [node_id],
        }

    impact = market_impact_report(
        node_id,
        stress,
        min_effect_threshold=threshold,
    )
    in_degree = len(parent_edges(node_id))
    out_degree = len(child_edges(node_id))
    degree = in_degree + out_degree
    max_degree = max(1, 2 * (node_count - 1))
    structural_centrality = (
        0.0
        if node_count <= 1
        else round(degree / max_degree, 4)
    )
    downstream_reachable_count = sum(
        1
        for candidate in list_nodes()
        if candidate != node_id
        and abs(total_path_effect(node_id, candidate) * stress) >= threshold
    )
    downstream_reach_ratio = (
        0.0
        if node_count <= 1
        else round(downstream_reachable_count / max(1, node_count - 1), 4)
    )

    downstream_nodes = [
        item
        for item in impact["affected_nodes"]
        if isinstance(item.get("node_id"), str) and item["node_id"] != node_id
    ]
    downstream_total_abs = round(
        sum(float(item["abs_delta"]) for item in downstream_nodes),
        4,
    )
    downstream_max_abs = round(
        max((float(item["abs_delta"]) for item in downstream_nodes), default=0.0),
        4,
    )
    concentration_risk = (
        0.0
        if downstream_total_abs == 0
        else round(downstream_max_abs / downstream_total_abs, 4)
    )

    impact_intensity = float(impact["market_change_level"])
    propagation_breadth = (
        0.0
        if node_count <= 1
        else round(
            float(impact["downstream_affected_node_count"]) / max(1, node_count - 1),
            4,
        )
    )
    systemic_risk_score = round(
        0.45 * impact_intensity
        + 0.30 * propagation_breadth
        + 0.15 * concentration_risk
        + 0.10 * structural_centrality,
        4,
    )

    return {
        "node_id": node_id,
        "node_known": True,
        "stress_delta": round(stress, 4),
        "min_effect_threshold": round(threshold, 6),
        "node_count": node_count,
        "in_degree": in_degree,
        "out_degree": out_degree,
        "degree": degree,
        "downstream_reachable_count": downstream_reachable_count,
        "downstream_reach_ratio": downstream_reach_ratio,
        "affected_node_count": int(impact["affected_node_count"]),
        "downstream_affected_node_count": int(impact["downstream_affected_node_count"]),
        "unaffected_node_count": int(impact["unaffected_node_count"]),
        "total_absolute_change": float(impact["total_absolute_change"]),
        "average_absolute_change": float(impact["average_absolute_change"]),
        "market_change_level": impact_intensity,
        "max_affected_node": impact["max_affected_node"],
        "max_affected_change": impact["max_affected_change"],
        "max_affected_change_abs": impact["max_affected_change_abs"],
        "impact_intensity": impact_intensity,
        "propagation_breadth": propagation_breadth,
        "concentration_risk": concentration_risk,
        "structural_centrality": structural_centrality,
        "systemic_risk_score": systemic_risk_score,
        "systemic_risk_level": _systemic_risk_level(systemic_risk_score),
        "affected_nodes": impact["affected_nodes"],
        "missing_nodes": [],
    }


def intervention_ranking_report(
    outcome_node: str,
    intervention_delta: float,
    *,
    candidate_nodes: list[str] | None = None,
    top_k: int = 5,
    min_effect_threshold: float = 0.0001,
) -> dict[str, object]:
    bounded_top_k = max(1, int(top_k))
    threshold = max(0.0, float(min_effect_threshold))
    intervention = float(intervention_delta)
    outcome_known = outcome_node in NODE_BASELINES

    selected_nodes = (
        _dedupe_preserve(list_nodes())
        if candidate_nodes is None
        else _dedupe_preserve([node_id for node_id in candidate_nodes if isinstance(node_id, str)])
    )
    missing_candidate_nodes = [
        node_id for node_id in selected_nodes if node_id not in NODE_BASELINES
    ]
    known_candidates = [
        node_id for node_id in selected_nodes if node_id in NODE_BASELINES
    ]

    if not outcome_known:
        return {
            "outcome_node": outcome_node,
            "outcome_known": False,
            "intervention_delta": round(intervention, 4),
            "min_effect_threshold": round(threshold, 6),
            "top_k": bounded_top_k,
            "candidate_count": len(selected_nodes),
            "evaluated_candidate_count": len(known_candidates),
            "ranked_candidate_count": 0,
            "missing_nodes": [outcome_node],
            "missing_candidate_nodes": missing_candidate_nodes,
            "rankings": [],
        }

    rows: list[dict[str, object]] = []
    for candidate in known_candidates:
        effect_on_outcome = round(total_path_effect(candidate, outcome_node) * intervention, 4)
        impact = market_impact_report(
            candidate,
            intervention,
            min_effect_threshold=threshold,
        )
        rows.append(
            {
                "candidate_node": candidate,
                "effect_on_outcome": effect_on_outcome,
                "abs_effect_on_outcome": round(abs(effect_on_outcome), 4),
                "affected_node_count": int(impact["affected_node_count"]),
                "downstream_affected_node_count": int(impact["downstream_affected_node_count"]),
                "market_change_level": float(impact["market_change_level"]),
                "total_absolute_change": float(impact["total_absolute_change"]),
                "max_affected_node": impact["max_affected_node"],
                "max_affected_change_abs": impact["max_affected_change_abs"],
            }
        )

    rows = sorted(
        rows,
        key=lambda item: (
            -float(item["abs_effect_on_outcome"]),
            -float(item["market_change_level"]),
            str(item["candidate_node"]),
        ),
    )
    ranked = rows[:bounded_top_k]
    rankings = [
        {
            "rank": index + 1,
            **row,
        }
        for index, row in enumerate(ranked)
    ]

    return {
        "outcome_node": outcome_node,
        "outcome_known": True,
        "intervention_delta": round(intervention, 4),
        "min_effect_threshold": round(threshold, 6),
        "top_k": bounded_top_k,
        "candidate_count": len(selected_nodes),
        "evaluated_candidate_count": len(known_candidates),
        "ranked_candidate_count": len(rankings),
        "missing_nodes": [],
        "missing_candidate_nodes": missing_candidate_nodes,
        "rankings": rankings,
    }


def node_criticality_ranking_report(
    *,
    candidate_nodes: list[str] | None = None,
    stress_delta: float = 1.0,
    top_k: int = 5,
    min_effect_threshold: float = 0.0001,
) -> dict[str, object]:
    bounded_top_k = max(1, int(top_k))
    selected_nodes, known_candidates, missing_candidate_nodes = _normalize_candidate_nodes(
        candidate_nodes
    )
    rows: list[dict[str, object]] = []
    for candidate in known_candidates:
        risk = node_systemic_risk_report(
            candidate,
            stress_delta,
            min_effect_threshold=min_effect_threshold,
        )
        rows.append(
            {
                "node_id": candidate,
                "systemic_risk_score": float(risk["systemic_risk_score"]),
                "systemic_risk_level": str(risk["systemic_risk_level"]),
                "market_change_level": float(risk["market_change_level"]),
                "affected_node_count": int(risk["affected_node_count"]),
                "downstream_affected_node_count": int(risk["downstream_affected_node_count"]),
                "degree": int(risk["degree"]),
                "structural_centrality": float(risk["structural_centrality"]),
                "concentration_risk": float(risk["concentration_risk"]),
            }
        )

    rows = sorted(
        rows,
        key=lambda item: (
            -float(item["systemic_risk_score"]),
            -float(item["market_change_level"]),
            str(item["node_id"]),
        ),
    )
    ranked = rows[:bounded_top_k]
    rankings = [
        {
            "rank": index + 1,
            **row,
        }
        for index, row in enumerate(ranked)
    ]
    return {
        "stress_delta": round(float(stress_delta), 4),
        "min_effect_threshold": round(float(max(0.0, min_effect_threshold)), 6),
        "top_k": bounded_top_k,
        "candidate_count": len(selected_nodes),
        "evaluated_candidate_count": len(known_candidates),
        "ranked_node_count": len(rankings),
        "missing_candidate_nodes": missing_candidate_nodes,
        "rankings": rankings,
    }


def edge_criticality_ranking_report(
    *,
    top_k: int = 5,
) -> dict[str, object]:
    bounded_top_k = max(1, int(top_k))
    baseline = _pairwise_absolute_influence()
    rows: list[dict[str, object]] = []
    for edge in EDGES:
        disabled_edges = {(edge.source, edge.target)}
        perturbed = _pairwise_absolute_influence(disabled_edges=disabled_edges)
        influence_loss = round(max(0.0, baseline - perturbed), 4)
        influence_loss_ratio = (
            0.0
            if baseline == 0
            else round(influence_loss / baseline, 4)
        )
        revenue_loss = 0.0
        for source in list_nodes():
            if source == "revenue":
                continue
            before = abs(total_path_effect(source, "revenue"))
            after = abs(
                _total_path_effect_custom(
                    source,
                    "revenue",
                    disabled_edges=disabled_edges,
                )
            )
            revenue_loss += max(0.0, before - after)
        revenue_loss = round(revenue_loss, 4)
        rows.append(
            {
                "source_node_id": edge.source,
                "target_node_id": edge.target,
                "edge_weight": edge.weight,
                "influence_loss": influence_loss,
                "influence_loss_ratio": influence_loss_ratio,
                "revenue_influence_loss": revenue_loss,
            }
        )

    rows = sorted(
        rows,
        key=lambda item: (
            -float(item["influence_loss"]),
            -float(item["revenue_influence_loss"]),
            str(item["source_node_id"]),
            str(item["target_node_id"]),
        ),
    )
    ranked = rows[:bounded_top_k]
    rankings = [
        {
            "rank": index + 1,
            **row,
        }
        for index, row in enumerate(ranked)
    ]
    return {
        "top_k": bounded_top_k,
        "edge_count": len(EDGES),
        "baseline_pairwise_absolute_influence": baseline,
        "ranked_edge_count": len(rankings),
        "rankings": rankings,
    }


def goal_seek_intervention_report(
    outcome_node: str,
    target_outcome_change: float,
    *,
    candidate_nodes: list[str] | None = None,
    max_plans: int = 5,
    min_effect_threshold: float = 0.0001,
) -> dict[str, object]:
    bounded_max_plans = max(1, int(max_plans))
    threshold = max(0.0, float(min_effect_threshold))
    target_change = float(target_outcome_change)
    outcome_known = outcome_node in NODE_BASELINES

    selected_nodes, known_candidates, missing_candidate_nodes = _normalize_candidate_nodes(
        candidate_nodes
    )
    if not outcome_known:
        return {
            "outcome_node": outcome_node,
            "outcome_known": False,
            "target_outcome_change": round(target_change, 4),
            "min_effect_threshold": round(threshold, 6),
            "max_plans": bounded_max_plans,
            "candidate_count": len(selected_nodes),
            "evaluated_candidate_count": len(known_candidates),
            "plan_count": 0,
            "achievable": False,
            "best_plan": None,
            "missing_nodes": [outcome_node],
            "missing_candidate_nodes": missing_candidate_nodes,
            "plans": [],
        }

    rows: list[dict[str, object]] = []
    for candidate in known_candidates:
        effect_per_unit = total_path_effect(candidate, outcome_node)
        if abs(effect_per_unit) < threshold:
            continue
        required_delta = target_change / effect_per_unit
        impact = market_impact_report(
            candidate,
            required_delta,
            min_effect_threshold=threshold,
        )
        rows.append(
            {
                "candidate_node": candidate,
                "effect_per_unit": round(effect_per_unit, 4),
                "required_intervention_delta": round(required_delta, 4),
                "expected_outcome_change": round(effect_per_unit * required_delta, 4),
                "market_change_level": float(impact["market_change_level"]),
                "total_absolute_change": float(impact["total_absolute_change"]),
                "affected_node_count": int(impact["affected_node_count"]),
                "max_affected_node": impact["max_affected_node"],
                "max_affected_change_abs": impact["max_affected_change_abs"],
            }
        )

    rows = sorted(
        rows,
        key=lambda item: (
            abs(float(item["required_intervention_delta"])),
            float(item["market_change_level"]),
            str(item["candidate_node"]),
        ),
    )
    limited = rows[:bounded_max_plans]
    plans = [
        {
            "rank": index + 1,
            **row,
        }
        for index, row in enumerate(limited)
    ]
    return {
        "outcome_node": outcome_node,
        "outcome_known": True,
        "target_outcome_change": round(target_change, 4),
        "min_effect_threshold": round(threshold, 6),
        "max_plans": bounded_max_plans,
        "candidate_count": len(selected_nodes),
        "evaluated_candidate_count": len(known_candidates),
        "plan_count": len(plans),
        "achievable": len(plans) > 0,
        "best_plan": plans[0] if plans else None,
        "missing_nodes": [],
        "missing_candidate_nodes": missing_candidate_nodes,
        "plans": plans,
    }


def budgeted_intervention_optimizer_report(
    outcome_node: str,
    budget: float,
    *,
    objective: str = "increase",
    candidate_nodes: list[str] | None = None,
    max_allocations: int = 3,
    min_effect_threshold: float = 0.0001,
) -> dict[str, object]:
    threshold = max(0.0, float(min_effect_threshold))
    objective_normalized = "decrease" if str(objective).lower() == "decrease" else "increase"
    bounded_allocations = max(1, int(max_allocations))
    available_budget = max(0.0, abs(float(budget)))
    outcome_known = outcome_node in NODE_BASELINES

    selected_nodes, known_candidates, missing_candidate_nodes = _normalize_candidate_nodes(
        candidate_nodes
    )
    if not outcome_known:
        return {
            "outcome_node": outcome_node,
            "outcome_known": False,
            "objective": objective_normalized,
            "budget": round(available_budget, 4),
            "min_effect_threshold": round(threshold, 6),
            "max_allocations": bounded_allocations,
            "candidate_count": len(selected_nodes),
            "evaluated_candidate_count": len(known_candidates),
            "selected_candidate_count": 0,
            "expected_outcome_change": 0.0,
            "impact_summary": _empty_impact_summary(),
            "missing_nodes": [outcome_node],
            "missing_candidate_nodes": missing_candidate_nodes,
            "allocations": [],
            "interventions": [],
        }

    candidate_rows: list[dict[str, object]] = []
    for candidate in known_candidates:
        effect_per_unit = total_path_effect(candidate, outcome_node)
        if abs(effect_per_unit) < threshold:
            continue
        intervention_sign = (
            1.0 if effect_per_unit >= 0 else -1.0
        ) if objective_normalized == "increase" else (
            -1.0 if effect_per_unit >= 0 else 1.0
        )
        candidate_rows.append(
            {
                "candidate_node": candidate,
                "effect_per_unit": round(effect_per_unit, 4),
                "utility_per_budget": round(abs(effect_per_unit), 4),
                "intervention_sign": intervention_sign,
            }
        )

    candidate_rows = sorted(
        candidate_rows,
        key=lambda item: (
            -float(item["utility_per_budget"]),
            str(item["candidate_node"]),
        ),
    )
    selected = candidate_rows[:bounded_allocations]
    utility_total = sum(float(item["utility_per_budget"]) for item in selected)

    allocations: list[dict[str, object]] = []
    interventions: list[dict[str, float | str]] = []
    expected_outcome_change = 0.0
    for item in selected:
        utility = float(item["utility_per_budget"])
        if available_budget == 0:
            allocated_budget = 0.0
        elif utility_total == 0:
            allocated_budget = available_budget / len(selected)
        else:
            allocated_budget = available_budget * (utility / utility_total)
        intervention_delta = allocated_budget * float(item["intervention_sign"])
        effect_per_unit = float(item["effect_per_unit"])
        expected_change = effect_per_unit * intervention_delta
        expected_outcome_change += expected_change
        row = {
            "candidate_node": item["candidate_node"],
            "effect_per_unit": round(effect_per_unit, 4),
            "allocated_budget": round(allocated_budget, 4),
            "intervention_delta": round(intervention_delta, 4),
            "expected_outcome_contribution": round(expected_change, 4),
        }
        allocations.append(row)
        interventions.append(
            {
                "target_node": str(item["candidate_node"]),
                "intervention_delta": round(intervention_delta, 4),
            }
        )

    impact = multi_intervention_impact_report(
        [
            {
                "target_node": row["target_node"],
                "intervention_delta": row["intervention_delta"],
            }
            for row in interventions
        ],
        min_effect_threshold=threshold,
    )
    return {
        "outcome_node": outcome_node,
        "outcome_known": True,
        "objective": objective_normalized,
        "budget": round(available_budget, 4),
        "min_effect_threshold": round(threshold, 6),
        "max_allocations": bounded_allocations,
        "candidate_count": len(selected_nodes),
        "evaluated_candidate_count": len(known_candidates),
        "selected_candidate_count": len(selected),
        "expected_outcome_change": round(expected_outcome_change, 4),
        "impact_summary": {
            "affected_node_count": int(impact["affected_node_count"]),
            "market_change_level": float(impact["market_change_level"]),
            "total_absolute_change": float(impact["total_absolute_change"]),
            "max_affected_node": impact["max_affected_node"],
            "max_affected_change_abs": impact["max_affected_change_abs"],
        },
        "missing_nodes": [],
        "missing_candidate_nodes": missing_candidate_nodes,
        "allocations": allocations,
        "interventions": interventions,
    }


def pareto_intervention_frontier_report(
    outcome_node: str,
    intervention_delta: float,
    *,
    objective: str = "increase",
    candidate_nodes: list[str] | None = None,
    min_effect_threshold: float = 0.0001,
) -> dict[str, object]:
    threshold = max(0.0, float(min_effect_threshold))
    objective_normalized = "decrease" if str(objective).lower() == "decrease" else "increase"
    intervention = float(intervention_delta)
    outcome_known = outcome_node in NODE_BASELINES

    selected_nodes, known_candidates, missing_candidate_nodes = _normalize_candidate_nodes(
        candidate_nodes
    )
    if not outcome_known:
        return {
            "outcome_node": outcome_node,
            "outcome_known": False,
            "objective": objective_normalized,
            "intervention_delta": round(intervention, 4),
            "min_effect_threshold": round(threshold, 6),
            "candidate_count": len(selected_nodes),
            "evaluated_candidate_count": len(known_candidates),
            "frontier_count": 0,
            "missing_nodes": [outcome_node],
            "missing_candidate_nodes": missing_candidate_nodes,
            "frontier": [],
            "candidates": [],
        }

    rows: list[dict[str, object]] = []
    for candidate in known_candidates:
        effect = round(total_path_effect(candidate, outcome_node) * intervention, 4)
        if abs(effect) < threshold:
            continue
        utility = effect if objective_normalized == "increase" else -effect
        impact = market_impact_report(
            candidate,
            intervention,
            min_effect_threshold=threshold,
        )
        rows.append(
            {
                "candidate_node": candidate,
                "effect_on_outcome": effect,
                "utility": round(utility, 4),
                "market_change_level": float(impact["market_change_level"]),
                "total_absolute_change": float(impact["total_absolute_change"]),
                "affected_node_count": int(impact["affected_node_count"]),
            }
        )

    frontier_rows: list[dict[str, object]] = []
    for row in rows:
        utility = float(row["utility"])
        disruption = float(row["market_change_level"])
        dominated = False
        for other in rows:
            if other is row:
                continue
            other_utility = float(other["utility"])
            other_disruption = float(other["market_change_level"])
            if (
                other_utility >= utility
                and other_disruption <= disruption
                and (other_utility > utility or other_disruption < disruption)
            ):
                dominated = True
                break
        row_with_flag = {
            **row,
            "dominated": dominated,
        }
        if not dominated and utility > 0:
            frontier_rows.append(row_with_flag)

    frontier_rows = sorted(
        frontier_rows,
        key=lambda item: (
            -float(item["utility"]),
            float(item["market_change_level"]),
            str(item["candidate_node"]),
        ),
    )
    frontier = [
        {
            "frontier_rank": index + 1,
            **row,
        }
        for index, row in enumerate(frontier_rows)
    ]
    candidates = sorted(
        [{**row} for row in rows],
        key=lambda item: (
            -float(item["utility"]),
            float(item["market_change_level"]),
            str(item["candidate_node"]),
        ),
    )
    return {
        "outcome_node": outcome_node,
        "outcome_known": True,
        "objective": objective_normalized,
        "intervention_delta": round(intervention, 4),
        "min_effect_threshold": round(threshold, 6),
        "candidate_count": len(selected_nodes),
        "evaluated_candidate_count": len(known_candidates),
        "frontier_count": len(frontier),
        "missing_nodes": [],
        "missing_candidate_nodes": missing_candidate_nodes,
        "frontier": frontier,
        "candidates": candidates,
    }


def scenario_compare_report(
    scenarios: list[dict[str, object]],
    *,
    outcome_node: str | None = None,
    min_effect_threshold: float = 0.0001,
) -> dict[str, object]:
    threshold = max(0.0, float(min_effect_threshold))
    outcome_known = outcome_node in NODE_BASELINES if isinstance(outcome_node, str) else None

    scenario_rows: list[dict[str, object]] = []
    all_missing_nodes: list[str] = []
    for index, scenario in enumerate(scenarios):
        if not isinstance(scenario, dict):
            continue
        raw_name = scenario.get("name")
        name = (
            raw_name
            if isinstance(raw_name, str) and raw_name.strip()
            else f"scenario_{index + 1}"
        )
        raw_interventions = scenario.get("interventions")
        normalized_interventions, missing_nodes = _normalize_interventions(raw_interventions)
        all_missing_nodes.extend(missing_nodes)

        impact = multi_intervention_impact_report(
            normalized_interventions,
            min_effect_threshold=threshold,
        )
        if isinstance(outcome_node, str) and outcome_known:
            outcome_delta = _sum_outcome_delta(normalized_interventions, outcome_node)
        else:
            outcome_delta = None
        scenario_rows.append(
            {
                "name": name,
                "intervention_count": len(normalized_interventions),
                "evaluated_intervention_count": int(impact["evaluated_intervention_count"]),
                "missing_nodes": missing_nodes,
                "outcome_delta": outcome_delta,
                "affected_node_count": int(impact["affected_node_count"]),
                "market_change_level": float(impact["market_change_level"]),
                "total_absolute_change": float(impact["total_absolute_change"]),
                "max_affected_node": impact["max_affected_node"],
                "max_affected_change_abs": impact["max_affected_change_abs"],
            }
        )

    best_by_outcome = None
    if isinstance(outcome_node, str) and outcome_known and scenario_rows:
        comparable = [
            row
            for row in scenario_rows
            if isinstance(row.get("outcome_delta"), (float, int))
        ]
        if comparable:
            best_by_outcome = max(
                comparable,
                key=lambda item: (
                    float(item["outcome_delta"]),
                    -float(item["market_change_level"]),
                ),
            )

    least_disruptive = None
    if scenario_rows:
        least_disruptive = min(
            scenario_rows,
            key=lambda item: (
                float(item["market_change_level"]),
                -float(item.get("outcome_delta") or 0.0),
                str(item["name"]),
            ),
        )

    return {
        "scenario_count": len(scenario_rows),
        "outcome_node": outcome_node,
        "outcome_known": outcome_known,
        "min_effect_threshold": round(threshold, 6),
        "best_by_outcome": best_by_outcome,
        "least_disruptive": least_disruptive,
        "missing_nodes": _dedupe_preserve(all_missing_nodes),
        "scenarios": scenario_rows,
    }


def shock_cascade_simulation_report(
    target_node: str,
    shock_delta: float,
    *,
    steps: int = 3,
    damping: float = 0.6,
    min_effect_threshold: float = 0.0001,
    noise_scale: float = 0.0,
    random_seed: int | None = None,
) -> dict[str, object]:
    threshold = max(0.0, float(min_effect_threshold))
    bounded_steps = max(0, int(steps))
    damping_factor = float(damping)
    bounded_noise = max(0.0, float(noise_scale))
    shock = float(shock_delta)
    target_known = target_node in NODE_BASELINES
    if not target_known:
        return {
            "target_node": target_node,
            "target_known": False,
            "shock_delta": round(shock, 4),
            "steps_requested": bounded_steps,
            "steps_executed": 0,
            "damping": round(damping_factor, 4),
            "min_effect_threshold": round(threshold, 6),
            "noise_scale": round(bounded_noise, 4),
            "random_seed": random_seed,
            "affected_node_count": 0,
            "total_absolute_change": 0.0,
            "market_change_level": 0.0,
            "max_affected_node": None,
            "max_affected_change_abs": None,
            "steps": [],
            "aggregated_changes": [],
            "missing_nodes": [target_node],
        }

    rng = random.Random(random_seed)
    current_deltas: dict[str, float] = {target_node: shock}
    aggregated: dict[str, float] = {node_id: 0.0 for node_id in list_nodes()}
    step_rows: list[dict[str, object]] = []

    for step_index in range(bounded_steps + 1):
        active = {
            node_id: delta
            for node_id, delta in current_deltas.items()
            if abs(delta) >= threshold
        }
        if not active:
            break

        for node_id, delta in active.items():
            aggregated[node_id] += delta

        node_changes = []
        for node_id, delta in sorted(
            active.items(),
            key=lambda item: (-abs(item[1]), item[0]),
        ):
            baseline = NODE_BASELINES[node_id]
            relative_change = None if baseline == 0 else round(delta / baseline, 4)
            node_changes.append(
                {
                    "node_id": node_id,
                    "delta": round(delta, 4),
                    "abs_delta": round(abs(delta), 4),
                    "baseline": baseline,
                    "relative_change": relative_change,
                }
            )
        step_rows.append(
            {
                "step": step_index,
                "active_node_count": len(node_changes),
                "node_changes": node_changes,
            }
        )
        if step_index == bounded_steps:
            continue

        next_deltas: dict[str, float] = {}
        for source_node, source_delta in active.items():
            for edge in child_edges(source_node):
                propagated = source_delta * edge.weight * damping_factor
                if bounded_noise > 0:
                    jitter = rng.uniform(-bounded_noise, bounded_noise)
                    propagated += propagated * jitter
                next_deltas[edge.target] = next_deltas.get(edge.target, 0.0) + propagated
        current_deltas = next_deltas

    aggregated_rows = []
    for node_id in list_nodes():
        delta = round(aggregated[node_id], 4)
        abs_delta = round(abs(delta), 4)
        if abs_delta < threshold:
            continue
        baseline = NODE_BASELINES[node_id]
        relative_change = None if baseline == 0 else round(delta / baseline, 4)
        aggregated_rows.append(
            {
                "node_id": node_id,
                "delta": delta,
                "abs_delta": abs_delta,
                "baseline": baseline,
                "relative_change": relative_change,
            }
        )
    aggregated_rows = sorted(
        aggregated_rows,
        key=lambda item: (-float(item["abs_delta"]), str(item["node_id"])),
    )
    total_abs_change = round(
        sum(float(item["abs_delta"]) for item in aggregated_rows),
        4,
    )
    baseline_total = _baseline_total_abs()
    market_change_level = (
        0.0
        if baseline_total == 0
        else round(total_abs_change / baseline_total, 4)
    )
    max_row = aggregated_rows[0] if aggregated_rows else None
    return {
        "target_node": target_node,
        "target_known": True,
        "shock_delta": round(shock, 4),
        "steps_requested": bounded_steps,
        "steps_executed": len(step_rows),
        "damping": round(damping_factor, 4),
        "min_effect_threshold": round(threshold, 6),
        "noise_scale": round(bounded_noise, 4),
        "random_seed": random_seed,
        "affected_node_count": len(aggregated_rows),
        "total_absolute_change": total_abs_change,
        "market_change_level": market_change_level,
        "max_affected_node": None if max_row is None else max_row["node_id"],
        "max_affected_change_abs": None if max_row is None else max_row["abs_delta"],
        "steps": step_rows,
        "aggregated_changes": aggregated_rows,
        "missing_nodes": [],
    }


def resilience_report(
    *,
    top_k: int = 5,
    min_effect_threshold: float = 0.0001,
) -> dict[str, object]:
    bounded_top_k = max(1, int(top_k))
    threshold = max(0.0, float(min_effect_threshold))
    node_ids = list_nodes()
    possible_pairs = len(node_ids) * max(0, len(node_ids) - 1)
    baseline_reachability = _pairwise_reachability(min_effect_threshold=threshold)

    node_rows: list[dict[str, object]] = []
    for node_id in node_ids:
        reachable_after = _pairwise_reachability(
            disabled_nodes={node_id},
            min_effect_threshold=threshold,
        )
        reachability_loss = max(0, baseline_reachability - reachable_after)
        loss_ratio = (
            0.0
            if baseline_reachability == 0
            else round(reachability_loss / baseline_reachability, 4)
        )
        node_rows.append(
            {
                "node_id": node_id,
                "reachable_pair_count_after_failure": reachable_after,
                "reachability_loss": reachability_loss,
                "reachability_loss_ratio": loss_ratio,
            }
        )

    edge_rows: list[dict[str, object]] = []
    for edge in EDGES:
        reachable_after = _pairwise_reachability(
            disabled_edges={(edge.source, edge.target)},
            min_effect_threshold=threshold,
        )
        reachability_loss = max(0, baseline_reachability - reachable_after)
        loss_ratio = (
            0.0
            if baseline_reachability == 0
            else round(reachability_loss / baseline_reachability, 4)
        )
        edge_rows.append(
            {
                "source_node_id": edge.source,
                "target_node_id": edge.target,
                "reachable_pair_count_after_failure": reachable_after,
                "reachability_loss": reachability_loss,
                "reachability_loss_ratio": loss_ratio,
            }
        )

    node_rows = sorted(
        node_rows,
        key=lambda item: (
            -float(item["reachability_loss_ratio"]),
            str(item["node_id"]),
        ),
    )
    edge_rows = sorted(
        edge_rows,
        key=lambda item: (
            -float(item["reachability_loss_ratio"]),
            str(item["source_node_id"]),
            str(item["target_node_id"]),
        ),
    )
    resilience_index = (
        1.0
        if not node_rows
        else round(
            sum(1.0 - float(item["reachability_loss_ratio"]) for item in node_rows)
            / len(node_rows),
            4,
        )
    )
    return {
        "node_count": len(node_ids),
        "edge_count": len(EDGES),
        "possible_ordered_pairs": possible_pairs,
        "min_effect_threshold": round(threshold, 6),
        "baseline_reachable_pair_count": baseline_reachability,
        "baseline_reachability_ratio": (
            0.0 if possible_pairs == 0 else round(baseline_reachability / possible_pairs, 4)
        ),
        "resilience_index": resilience_index,
        "most_fragile_node": node_rows[0] if node_rows else None,
        "most_fragile_edge": edge_rows[0] if edge_rows else None,
        "node_failure_rankings": node_rows[:bounded_top_k],
        "edge_failure_rankings": edge_rows[:bounded_top_k],
    }


def target_vulnerability_report(
    target_node: str,
    *,
    shock_delta: float = 1.0,
    candidate_sources: list[str] | None = None,
    top_k: int = 5,
    min_effect_threshold: float = 0.0001,
) -> dict[str, object]:
    threshold = max(0.0, float(min_effect_threshold))
    bounded_top_k = max(1, int(top_k))
    shock = float(shock_delta)
    target_known = target_node in NODE_BASELINES
    selected_sources, known_sources, missing_sources = _normalize_candidate_nodes(candidate_sources)
    known_sources = [source for source in known_sources if source != target_node]

    if not target_known:
        return {
            "target_node": target_node,
            "target_known": False,
            "shock_delta": round(shock, 4),
            "min_effect_threshold": round(threshold, 6),
            "top_k": bounded_top_k,
            "candidate_source_count": len(selected_sources),
            "evaluated_source_count": len(known_sources),
            "vulnerability_source_count": 0,
            "concentration_index": 0.0,
            "missing_nodes": [target_node],
            "missing_candidate_sources": missing_sources,
            "rankings": [],
        }

    rows: list[dict[str, object]] = []
    for source in known_sources:
        effect = round(total_path_effect(source, target_node) * shock, 4)
        abs_effect = round(abs(effect), 4)
        if abs_effect < threshold:
            continue
        paths = find_paths(source, target_node, 50)
        shortest_path_length = (
            min((len(path) - 1 for path in paths), default=None)
            if paths
            else None
        )
        rows.append(
            {
                "source_node": source,
                "effect_on_target": effect,
                "abs_effect_on_target": abs_effect,
                "path_count": len(paths),
                "shortest_path_length": shortest_path_length,
            }
        )

    rows = sorted(
        rows,
        key=lambda item: (
            -float(item["abs_effect_on_target"]),
            str(item["source_node"]),
        ),
    )
    total_abs = sum(float(item["abs_effect_on_target"]) for item in rows)
    concentration = (
        0.0
        if total_abs == 0
        else round(
            sum(
                (float(item["abs_effect_on_target"]) / total_abs) ** 2
                for item in rows
            ),
            4,
        )
    )
    rankings = []
    for index, row in enumerate(rows[:bounded_top_k]):
        share = (
            None
            if total_abs == 0
            else round(float(row["abs_effect_on_target"]) / total_abs, 4)
        )
        rankings.append(
            {
                "rank": index + 1,
                **row,
                "share_of_total_vulnerability": share,
            }
        )
    return {
        "target_node": target_node,
        "target_known": True,
        "shock_delta": round(shock, 4),
        "min_effect_threshold": round(threshold, 6),
        "top_k": bounded_top_k,
        "candidate_source_count": len(selected_sources),
        "evaluated_source_count": len(known_sources),
        "vulnerability_source_count": len(rows),
        "concentration_index": concentration,
        "missing_nodes": [],
        "missing_candidate_sources": missing_sources,
        "rankings": rankings,
    }


def bottleneck_report(
    *,
    top_k: int = 5,
    max_paths_per_pair: int = 20,
) -> dict[str, object]:
    bounded_top_k = max(1, int(top_k))
    bounded_paths = max(1, int(max_paths_per_pair))
    node_ids = list_nodes()
    possible_pairs = len(node_ids) * max(0, len(node_ids) - 1)

    node_counts: dict[str, int] = {}
    node_pair_coverage: dict[str, set[tuple[str, str]]] = {}
    edge_counts: dict[tuple[str, str], int] = {}
    edge_pair_coverage: dict[tuple[str, str], set[tuple[str, str]]] = {}
    total_paths_considered = 0

    for source in node_ids:
        for target in node_ids:
            if source == target:
                continue
            paths = find_paths(source, target, bounded_paths)
            if not paths:
                continue
            pair = (source, target)
            for path in paths:
                total_paths_considered += 1
                for node_id in path[1:-1]:
                    node_counts[node_id] = node_counts.get(node_id, 0) + 1
                    node_pair_coverage.setdefault(node_id, set()).add(pair)
                for index in range(len(path) - 1):
                    edge_key = (path[index], path[index + 1])
                    edge_counts[edge_key] = edge_counts.get(edge_key, 0) + 1
                    edge_pair_coverage.setdefault(edge_key, set()).add(pair)

    node_rows = [
        {
            "node_id": node_id,
            "path_occurrence_count": count,
            "source_target_pair_count": len(node_pair_coverage.get(node_id, set())),
            "pair_coverage_ratio": (
                0.0
                if possible_pairs == 0
                else round(len(node_pair_coverage.get(node_id, set())) / possible_pairs, 4)
            ),
        }
        for node_id, count in node_counts.items()
    ]
    edge_rows = [
        {
            "source_node_id": edge_key[0],
            "target_node_id": edge_key[1],
            "path_occurrence_count": count,
            "source_target_pair_count": len(edge_pair_coverage.get(edge_key, set())),
            "pair_coverage_ratio": (
                0.0
                if possible_pairs == 0
                else round(len(edge_pair_coverage.get(edge_key, set())) / possible_pairs, 4)
            ),
        }
        for edge_key, count in edge_counts.items()
    ]
    node_rows = sorted(
        node_rows,
        key=lambda item: (
            -int(item["path_occurrence_count"]),
            -int(item["source_target_pair_count"]),
            str(item["node_id"]),
        ),
    )
    edge_rows = sorted(
        edge_rows,
        key=lambda item: (
            -int(item["path_occurrence_count"]),
            -int(item["source_target_pair_count"]),
            str(item["source_node_id"]),
            str(item["target_node_id"]),
        ),
    )
    return {
        "node_count": len(node_ids),
        "edge_count": len(EDGES),
        "max_paths_per_pair": bounded_paths,
        "possible_ordered_pairs": possible_pairs,
        "total_paths_considered": total_paths_considered,
        "top_nodes": node_rows[:bounded_top_k],
        "top_edges": edge_rows[:bounded_top_k],
    }


def influence_matrix_report(
    *,
    node_ids: list[str] | None = None,
) -> dict[str, object]:
    selected_nodes, known_nodes, missing_nodes = _normalize_candidate_nodes(node_ids)
    if not known_nodes:
        return {
            "selected_node_count": len(selected_nodes),
            "evaluated_node_count": 0,
            "missing_nodes": missing_nodes,
            "nodes": [],
            "matrix": [],
            "matrix_rows": [],
            "out_strength": [],
            "in_strength": [],
            "strongest_source_node": None,
            "strongest_target_node": None,
        }

    matrix: list[list[float]] = []
    matrix_rows: list[dict[str, object]] = []
    out_strength: list[dict[str, object]] = []
    in_strength_accumulator = {node_id: 0.0 for node_id in known_nodes}

    for source in known_nodes:
        row_values: list[float] = []
        influences = []
        out_abs = 0.0
        for target in known_nodes:
            effect = round(total_path_effect(source, target), 4)
            row_values.append(effect)
            influences.append({"target_node": target, "effect": effect})
            if source != target:
                out_abs += abs(effect)
                in_strength_accumulator[target] += abs(effect)
        matrix.append(row_values)
        matrix_rows.append(
            {
                "source_node": source,
                "influences": influences,
                "total_outgoing_abs_effect": round(out_abs, 4),
            }
        )
        out_strength.append(
            {
                "node_id": source,
                "total_outgoing_abs_effect": round(out_abs, 4),
            }
        )

    in_strength = [
        {
            "node_id": node_id,
            "total_incoming_abs_effect": round(value, 4),
        }
        for node_id, value in in_strength_accumulator.items()
    ]
    out_strength = sorted(
        out_strength,
        key=lambda item: (
            -float(item["total_outgoing_abs_effect"]),
            str(item["node_id"]),
        ),
    )
    in_strength = sorted(
        in_strength,
        key=lambda item: (
            -float(item["total_incoming_abs_effect"]),
            str(item["node_id"]),
        ),
    )
    return {
        "selected_node_count": len(selected_nodes),
        "evaluated_node_count": len(known_nodes),
        "missing_nodes": missing_nodes,
        "nodes": known_nodes,
        "matrix": matrix,
        "matrix_rows": matrix_rows,
        "out_strength": out_strength,
        "in_strength": in_strength,
        "strongest_source_node": out_strength[0] if out_strength else None,
        "strongest_target_node": in_strength[0] if in_strength else None,
    }


def intervention_battle_report(
    outcome_node: str,
    plan_a: dict[str, object],
    plan_b: dict[str, object],
    *,
    min_effect_threshold: float = 0.0001,
    disruption_penalty: float = 1.0,
) -> dict[str, object]:
    threshold = max(0.0, float(min_effect_threshold))
    penalty = max(0.0, float(disruption_penalty))
    outcome_known = outcome_node in NODE_BASELINES

    plan_a_normalized = _normalize_battle_plan("plan_a", plan_a)
    plan_b_normalized = _normalize_battle_plan("plan_b", plan_b)

    summary_a = _battle_plan_summary(
        plan_a_normalized,
        outcome_node=outcome_node,
        outcome_known=outcome_known,
        min_effect_threshold=threshold,
        disruption_penalty=penalty,
    )
    summary_b = _battle_plan_summary(
        plan_b_normalized,
        outcome_node=outcome_node,
        outcome_known=outcome_known,
        min_effect_threshold=threshold,
        disruption_penalty=penalty,
    )

    combined_interventions = (
        plan_a_normalized["interventions"] + plan_b_normalized["interventions"]
    )
    combined_impact = multi_intervention_impact_report(
        combined_interventions,
        min_effect_threshold=threshold,
    )
    combined_outcome_change = (
        _sum_outcome_delta(combined_interventions, outcome_node)
        if outcome_known
        else None
    )

    score_a = summary_a.get("battle_score")
    score_b = summary_b.get("battle_score")
    winner = "draw"
    if isinstance(score_a, (float, int)) and isinstance(score_b, (float, int)):
        if score_a > score_b:
            winner = str(summary_a["name"])
        elif score_b > score_a:
            winner = str(summary_b["name"])

    return {
        "outcome_node": outcome_node,
        "outcome_known": outcome_known,
        "min_effect_threshold": round(threshold, 6),
        "disruption_penalty": round(penalty, 4),
        "winner": winner,
        "plan_a": summary_a,
        "plan_b": summary_b,
        "combined": {
            "intervention_count": len(combined_interventions),
            "outcome_change": combined_outcome_change,
            "affected_node_count": int(combined_impact["affected_node_count"]),
            "market_change_level": float(combined_impact["market_change_level"]),
            "total_absolute_change": float(combined_impact["total_absolute_change"]),
            "max_affected_node": combined_impact["max_affected_node"],
            "max_affected_change_abs": combined_impact["max_affected_change_abs"],
            "missing_nodes": combined_impact["missing_nodes"],
        },
        "missing_nodes": [] if outcome_known else [outcome_node],
    }


def _normalize_battle_plan(name_fallback: str, payload: dict[str, object]) -> dict[str, object]:
    name = payload.get("name") if isinstance(payload.get("name"), str) else name_fallback
    interventions, missing_nodes = _normalize_interventions(payload.get("interventions"))
    return {
        "name": name,
        "interventions": interventions,
        "missing_nodes": missing_nodes,
    }


def _battle_plan_summary(
    normalized_plan: dict[str, object],
    *,
    outcome_node: str,
    outcome_known: bool,
    min_effect_threshold: float,
    disruption_penalty: float,
) -> dict[str, object]:
    interventions = normalized_plan["interventions"]
    if not isinstance(interventions, list):
        interventions = []

    impact = multi_intervention_impact_report(
        interventions,
        min_effect_threshold=min_effect_threshold,
    )
    outcome_change = (
        _sum_outcome_delta(interventions, outcome_node)
        if outcome_known
        else None
    )
    battle_score = (
        None
        if outcome_change is None
        else round(
            outcome_change - disruption_penalty * float(impact["market_change_level"]),
            4,
        )
    )
    return {
        "name": normalized_plan["name"],
        "intervention_count": len(interventions),
        "evaluated_intervention_count": int(impact["evaluated_intervention_count"]),
        "missing_nodes": normalized_plan["missing_nodes"],
        "outcome_change": outcome_change,
        "affected_node_count": int(impact["affected_node_count"]),
        "market_change_level": float(impact["market_change_level"]),
        "total_absolute_change": float(impact["total_absolute_change"]),
        "max_affected_node": impact["max_affected_node"],
        "max_affected_change_abs": impact["max_affected_change_abs"],
        "battle_score": battle_score,
    }


def _empty_impact_summary() -> dict[str, object]:
    return {
        "affected_node_count": 0,
        "market_change_level": 0.0,
        "total_absolute_change": 0.0,
        "max_affected_node": None,
        "max_affected_change_abs": None,
    }


def _normalize_candidate_nodes(
    candidate_nodes: list[str] | None,
) -> tuple[list[str], list[str], list[str]]:
    selected_nodes = (
        _dedupe_preserve(list_nodes())
        if candidate_nodes is None
        else _dedupe_preserve([node_id for node_id in candidate_nodes if isinstance(node_id, str)])
    )
    missing_nodes = [node_id for node_id in selected_nodes if node_id not in NODE_BASELINES]
    known_nodes = [node_id for node_id in selected_nodes if node_id in NODE_BASELINES]
    return selected_nodes, known_nodes, missing_nodes


def _normalize_interventions(
    raw_interventions: object,
) -> tuple[list[dict[str, object]], list[str]]:
    if not isinstance(raw_interventions, list):
        return [], []
    interventions: list[dict[str, object]] = []
    missing_nodes: list[str] = []
    for item in raw_interventions:
        if not isinstance(item, dict):
            continue
        target_node = item.get("target_node")
        if not isinstance(target_node, str) or not target_node:
            continue
        try:
            intervention_delta = float(item.get("intervention_delta", 1.0))
        except (TypeError, ValueError):
            intervention_delta = 1.0
        known = target_node in NODE_BASELINES
        interventions.append(
            {
                "target_node": target_node,
                "target_known": known,
                "intervention_delta": round(intervention_delta, 4),
            }
        )
        if not known:
            missing_nodes.append(target_node)
    return interventions, _dedupe_preserve(missing_nodes)


def _sum_outcome_delta(interventions: list[dict[str, object]], outcome_node: str) -> float:
    total = 0.0
    for intervention in interventions:
        target_node = intervention.get("target_node")
        if not isinstance(target_node, str) or target_node not in NODE_BASELINES:
            continue
        try:
            intervention_delta = float(intervention.get("intervention_delta", 1.0))
        except (TypeError, ValueError):
            intervention_delta = 1.0
        total += total_path_effect(target_node, outcome_node) * intervention_delta
    return round(total, 4)


def _pairwise_absolute_influence(
    *,
    disabled_nodes: set[str] | None = None,
    disabled_edges: set[tuple[str, str]] | None = None,
) -> float:
    total = 0.0
    for source in list_nodes():
        if disabled_nodes and source in disabled_nodes:
            continue
        for target in list_nodes():
            if source == target:
                continue
            if disabled_nodes and target in disabled_nodes:
                continue
            total += abs(
                _total_path_effect_custom(
                    source,
                    target,
                    disabled_nodes=disabled_nodes,
                    disabled_edges=disabled_edges,
                )
            )
    return round(total, 4)


def _pairwise_reachability(
    *,
    disabled_nodes: set[str] | None = None,
    disabled_edges: set[tuple[str, str]] | None = None,
    min_effect_threshold: float = 0.0001,
) -> int:
    threshold = max(0.0, float(min_effect_threshold))
    reachable = 0
    for source in list_nodes():
        if disabled_nodes and source in disabled_nodes:
            continue
        for target in list_nodes():
            if source == target:
                continue
            if disabled_nodes and target in disabled_nodes:
                continue
            effect = abs(
                _total_path_effect_custom(
                    source,
                    target,
                    disabled_nodes=disabled_nodes,
                    disabled_edges=disabled_edges,
                )
            )
            if effect >= threshold:
                reachable += 1
    return reachable


def _total_path_effect_custom(
    source: str,
    target: str,
    *,
    disabled_nodes: set[str] | None = None,
    disabled_edges: set[tuple[str, str]] | None = None,
    edge_weight_overrides: dict[tuple[str, str], float] | None = None,
) -> float:
    if disabled_nodes and (source in disabled_nodes or target in disabled_nodes):
        return 0.0
    return round(
        _path_effects_custom(
            source,
            target,
            visited={source},
            disabled_nodes=disabled_nodes,
            disabled_edges=disabled_edges,
            edge_weight_overrides=edge_weight_overrides,
        ),
        4,
    )


def _path_effects_custom(
    source: str,
    target: str,
    *,
    visited: set[str],
    disabled_nodes: set[str] | None = None,
    disabled_edges: set[tuple[str, str]] | None = None,
    edge_weight_overrides: dict[tuple[str, str], float] | None = None,
) -> float:
    if source == target:
        return 1.0

    total = 0.0
    for edge in child_edges(source):
        edge_key = (edge.source, edge.target)
        if disabled_edges and edge_key in disabled_edges:
            continue
        if disabled_nodes and (edge.source in disabled_nodes or edge.target in disabled_nodes):
            continue
        if edge.target in visited:
            continue
        weight = edge.weight
        if edge_weight_overrides and edge_key in edge_weight_overrides:
            weight = edge_weight_overrides[edge_key]
        downstream = _path_effects_custom(
            edge.target,
            target,
            visited=visited | {edge.target},
            disabled_nodes=disabled_nodes,
            disabled_edges=disabled_edges,
            edge_weight_overrides=edge_weight_overrides,
        )
        if downstream:
            total += weight * downstream
    return total


def _baseline_total_abs() -> float:
    return round(sum(abs(value) for value in NODE_BASELINES.values()), 4)


def _dedupe_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _edge_weight(source: str, target: str) -> float:
    for edge in EDGES:
        if edge.source == source and edge.target == target:
            return edge.weight
    raise ValueError(f"Edge `{source} -> {target}` not found in toy graph.")


def _systemic_risk_level(score: float) -> str:
    if score >= 0.35:
        return "critical"
    if score >= 0.2:
        return "high"
    if score >= 0.08:
        return "medium"
    return "low"
