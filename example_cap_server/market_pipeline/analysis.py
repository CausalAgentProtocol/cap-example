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

    if verb == "extensions.example.node_criticality_ranking":
        rankings = calculation.get("rankings", [])
        best = rankings[0] if isinstance(rankings, list) and rankings else {}
        metrics.update(
            {
                "candidate_count": calculation.get("candidate_count"),
                "ranked_node_count": calculation.get("ranked_node_count"),
                "best_node_id": best.get("node_id") if isinstance(best, dict) else None,
                "best_systemic_risk_score": (
                    best.get("systemic_risk_score") if isinstance(best, dict) else None
                ),
            }
        )
        return metrics

    if verb == "extensions.example.edge_criticality_ranking":
        rankings = calculation.get("rankings", [])
        best = rankings[0] if isinstance(rankings, list) and rankings else {}
        metrics.update(
            {
                "edge_count": calculation.get("edge_count"),
                "ranked_edge_count": calculation.get("ranked_edge_count"),
                "top_edge": (
                    (
                        best.get("source_node_id"),
                        best.get("target_node_id"),
                    )
                    if isinstance(best, dict)
                    else None
                ),
                "top_influence_loss": (
                    best.get("influence_loss") if isinstance(best, dict) else None
                ),
            }
        )
        return metrics

    if verb == "extensions.example.goal_seek_intervention":
        best = calculation.get("best_plan", {})
        metrics.update(
            {
                "outcome_known": calculation.get("outcome_known"),
                "achievable": calculation.get("achievable"),
                "plan_count": calculation.get("plan_count"),
                "best_candidate_node": (
                    best.get("candidate_node") if isinstance(best, dict) else None
                ),
                "best_required_delta": (
                    best.get("required_intervention_delta") if isinstance(best, dict) else None
                ),
            }
        )
        return metrics

    if verb == "extensions.example.budgeted_intervention_optimizer":
        impact = calculation.get("impact_summary", {})
        metrics.update(
            {
                "outcome_known": calculation.get("outcome_known"),
                "selected_candidate_count": calculation.get("selected_candidate_count"),
                "expected_outcome_change": calculation.get("expected_outcome_change"),
                "market_change_level": (
                    impact.get("market_change_level") if isinstance(impact, dict) else None
                ),
            }
        )
        return metrics

    if verb == "extensions.example.pareto_intervention_frontier":
        frontier = calculation.get("frontier", [])
        top = frontier[0] if isinstance(frontier, list) and frontier else {}
        metrics.update(
            {
                "outcome_known": calculation.get("outcome_known"),
                "frontier_count": calculation.get("frontier_count"),
                "top_frontier_candidate": (
                    top.get("candidate_node") if isinstance(top, dict) else None
                ),
                "top_frontier_utility": top.get("utility") if isinstance(top, dict) else None,
            }
        )
        return metrics

    if verb == "extensions.example.scenario_compare":
        best = calculation.get("best_by_outcome", {})
        least_disruptive = calculation.get("least_disruptive", {})
        metrics.update(
            {
                "scenario_count": calculation.get("scenario_count"),
                "outcome_known": calculation.get("outcome_known"),
                "best_by_outcome": best.get("name") if isinstance(best, dict) else None,
                "least_disruptive": (
                    least_disruptive.get("name")
                    if isinstance(least_disruptive, dict)
                    else None
                ),
            }
        )
        return metrics

    if verb == "extensions.example.shock_cascade_simulation":
        metrics.update(
            {
                "target_known": calculation.get("target_known"),
                "steps_executed": calculation.get("steps_executed"),
                "affected_node_count": calculation.get("affected_node_count"),
                "market_change_level": calculation.get("market_change_level"),
                "max_affected_node": calculation.get("max_affected_node"),
            }
        )
        return metrics

    if verb == "extensions.example.resilience_report":
        metrics.update(
            {
                "resilience_index": calculation.get("resilience_index"),
                "baseline_reachable_pair_count": calculation.get("baseline_reachable_pair_count"),
                "most_fragile_node": (
                    calculation.get("most_fragile_node", {}).get("node_id")
                    if isinstance(calculation.get("most_fragile_node"), dict)
                    else None
                ),
                "most_fragile_edge": (
                    (
                        calculation.get("most_fragile_edge", {}).get("source_node_id"),
                        calculation.get("most_fragile_edge", {}).get("target_node_id"),
                    )
                    if isinstance(calculation.get("most_fragile_edge"), dict)
                    else None
                ),
            }
        )
        return metrics

    if verb == "extensions.example.target_vulnerability_report":
        rankings = calculation.get("rankings", [])
        best = rankings[0] if isinstance(rankings, list) and rankings else {}
        metrics.update(
            {
                "target_known": calculation.get("target_known"),
                "vulnerability_source_count": calculation.get("vulnerability_source_count"),
                "concentration_index": calculation.get("concentration_index"),
                "top_source_node": (
                    best.get("source_node") if isinstance(best, dict) else None
                ),
                "top_abs_effect_on_target": (
                    best.get("abs_effect_on_target") if isinstance(best, dict) else None
                ),
            }
        )
        return metrics

    if verb == "extensions.example.bottleneck_report":
        top_nodes = calculation.get("top_nodes", [])
        top_edges = calculation.get("top_edges", [])
        top_node = top_nodes[0] if isinstance(top_nodes, list) and top_nodes else {}
        top_edge = top_edges[0] if isinstance(top_edges, list) and top_edges else {}
        metrics.update(
            {
                "total_paths_considered": calculation.get("total_paths_considered"),
                "top_node": top_node.get("node_id") if isinstance(top_node, dict) else None,
                "top_edge": (
                    (
                        top_edge.get("source_node_id"),
                        top_edge.get("target_node_id"),
                    )
                    if isinstance(top_edge, dict)
                    else None
                ),
            }
        )
        return metrics

    if verb == "extensions.example.influence_matrix":
        metrics.update(
            {
                "evaluated_node_count": calculation.get("evaluated_node_count"),
                "strongest_source_node": (
                    calculation.get("strongest_source_node", {}).get("node_id")
                    if isinstance(calculation.get("strongest_source_node"), dict)
                    else None
                ),
                "strongest_target_node": (
                    calculation.get("strongest_target_node", {}).get("node_id")
                    if isinstance(calculation.get("strongest_target_node"), dict)
                    else None
                ),
            }
        )
        return metrics

    if verb == "extensions.example.intervention_battle":
        combined = calculation.get("combined", {})
        metrics.update(
            {
                "outcome_known": calculation.get("outcome_known"),
                "winner": calculation.get("winner"),
                "combined_market_change_level": (
                    combined.get("market_change_level")
                    if isinstance(combined, dict)
                    else None
                ),
                "combined_affected_node_count": (
                    combined.get("affected_node_count")
                    if isinstance(combined, dict)
                    else None
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
