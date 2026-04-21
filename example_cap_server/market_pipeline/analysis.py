from __future__ import annotations

from typing import Any

from example_cap_server.market_pipeline.models import ParsedCAPRequest


def build_analysis(
    *,
    parsed: ParsedCAPRequest,
    graph_excerpt: dict[str, Any],
    calculation: dict[str, Any],
    postprocess: dict[str, Any],
) -> dict[str, Any]:
    available_nodes = graph_excerpt.get("available_nodes", [])
    missing_nodes = graph_excerpt.get("missing_nodes", [])
    edges = graph_excerpt.get("edges", [])
    provider = graph_excerpt.get("provider", "unknown")
    retrieval_mode = graph_excerpt.get("retrieval_mode", "unknown")

    graph_metrics = {
        "provider": provider,
        "retrieval_mode": retrieval_mode,
        "requested_node_count": len(graph_excerpt.get("requested_nodes", [])),
        "available_node_count": len(available_nodes),
        "missing_node_count": len(missing_nodes),
        "edge_count": len(edges),
        "truncated": bool(graph_excerpt.get("truncated", False)),
    }

    calculation_metrics = _build_calculation_metrics(parsed.verb, calculation)
    postprocess_metrics = {
        "node_property_count": len(postprocess.get("node_properties", [])),
        "parent_neighbor_summary_count": len(postprocess.get("parent_neighbor_summary", [])),
    }

    return {
        "verb": parsed.verb,
        "graph_metrics": graph_metrics,
        "calculation_metrics": calculation_metrics,
        "postprocess_metrics": postprocess_metrics,
    }


def _build_calculation_metrics(verb: str, calculation: dict[str, Any]) -> dict[str, Any]:
    status = calculation.get("status", "success")
    metrics: dict[str, Any] = {"status": status}

    if verb == "observe.predict":
        drivers = calculation.get("drivers", [])
        metrics.update(
            {
                "prediction": calculation.get("prediction"),
                "driver_count": len(drivers) if isinstance(drivers, list) else None,
            }
        )
        return metrics

    if verb == "intervene.do":
        metrics.update(
            {
                "effect": calculation.get("effect"),
                "reachable": calculation.get("reachable"),
            }
        )
        return metrics

    if verb in {"graph.neighbors", "graph.markov_blanket"}:
        neighbors = calculation.get("neighbors", [])
        metrics.update(
            {
                "neighbor_count": len(neighbors) if isinstance(neighbors, list) else None,
                "truncated": calculation.get("truncated"),
            }
        )
        return metrics

    if verb == "graph.paths":
        metrics.update(
            {
                "connected": calculation.get("connected"),
                "path_count": calculation.get("path_count"),
            }
        )
        return metrics

    if verb == "extensions.example.connectivity_report":
        metrics.update(
            {
                "connected": calculation.get("connected"),
                "path_count": calculation.get("path_count"),
                "truncated": calculation.get("truncated"),
                "shortest_path_length": calculation.get("shortest_path_length"),
                "longest_path_length": calculation.get("longest_path_length"),
            }
        )
        return metrics

    if verb == "extensions.example.path_contribution_report":
        metrics.update(
            {
                "connected": calculation.get("connected"),
                "path_count": calculation.get("path_count"),
                "truncated": calculation.get("truncated"),
                "total_effect": calculation.get("total_effect"),
                "top_contributing_path_effect": calculation.get(
                    "top_contributing_path_effect"
                ),
            }
        )
        return metrics

    if verb == "extensions.example.market_impact":
        metrics.update(
            {
                "affected_node_count": calculation.get("affected_node_count"),
                "downstream_affected_node_count": calculation.get(
                    "downstream_affected_node_count"
                ),
                "market_change_level": calculation.get("market_change_level"),
                "max_affected_node": calculation.get("max_affected_node"),
                "max_affected_change_abs": calculation.get("max_affected_change_abs"),
            }
        )
        return metrics

    if verb == "extensions.example.node_systemic_risk":
        metrics.update(
            {
                "node_known": calculation.get("node_known"),
                "systemic_risk_score": calculation.get("systemic_risk_score"),
                "systemic_risk_level": calculation.get("systemic_risk_level"),
                "market_change_level": calculation.get("market_change_level"),
                "downstream_affected_node_count": calculation.get(
                    "downstream_affected_node_count"
                ),
            }
        )
        return metrics

    if verb == "extensions.example.multi_intervention_impact":
        metrics.update(
            {
                "intervention_count": calculation.get("intervention_count"),
                "evaluated_intervention_count": calculation.get(
                    "evaluated_intervention_count"
                ),
                "affected_node_count": calculation.get("affected_node_count"),
                "market_change_level": calculation.get("market_change_level"),
                "max_affected_node": calculation.get("max_affected_node"),
                "max_affected_change_abs": calculation.get("max_affected_change_abs"),
            }
        )
        return metrics

    if verb == "extensions.example.intervention_ranking":
        rankings = calculation.get("rankings", [])
        best = rankings[0] if isinstance(rankings, list) and rankings else {}
        metrics.update(
            {
                "candidate_count": calculation.get("candidate_count"),
                "evaluated_candidate_count": calculation.get("evaluated_candidate_count"),
                "ranked_candidate_count": calculation.get("ranked_candidate_count"),
                "best_candidate_node": (
                    best.get("candidate_node") if isinstance(best, dict) else None
                ),
                "best_effect_on_outcome": (
                    best.get("effect_on_outcome") if isinstance(best, dict) else None
                ),
            }
        )
        return metrics

    if verb in {"traverse.parents", "traverse.children"}:
        nodes = calculation.get("nodes", [])
        metrics.update({"node_count": len(nodes) if isinstance(nodes, list) else None})
        return metrics

    if verb == "meta.methods":
        methods = calculation.get("methods", [])
        metrics.update({"method_count": len(methods) if isinstance(methods, list) else None})
        return metrics

    return metrics
