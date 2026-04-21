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
