from __future__ import annotations

from typing import Any

from example_cap_server import toy_graph
from example_cap_server.integrations import load_cap_function_map
from example_cap_server.market_pipeline.models import ParsedCAPRequest
from example_cap_server.market_pipeline.utils import (
    coerce_float,
    coerce_int,
    dedupe_preserve,
    is_known_node,
    missing_nodes_result,
    require_node_param,
)

STATIC_VERB_CALCULATORS = {
    "meta.capabilities",
    "meta.methods",
    "extensions.example.dataset_profile",
    "extensions.example.dataset_density",
    "extensions.example.verb_catalog",
}

PROCEDURAL_VERB_CALCULATORS = {
    "observe.predict",
    "intervene.do",
    "graph.neighbors",
    "graph.markov_blanket",
    "graph.paths",
    "traverse.parents",
    "traverse.children",
    "extensions.example.connectivity_report",
    "extensions.example.path_contribution_report",
    "extensions.example.market_impact",
    "extensions.example.node_systemic_risk",
    "extensions.example.multi_intervention_impact",
    "extensions.example.intervention_ranking",
}

SUPPORTED_VERB_CALCULATORS = STATIC_VERB_CALCULATORS | PROCEDURAL_VERB_CALCULATORS


async def build_local_graph(
    parsed: ParsedCAPRequest,
    *,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if parsed.verb in STATIC_VERB_CALCULATORS:
        requested_nodes = dedupe_preserve(parsed.node_ids)
        return {
            "provider": "not_applicable",
            "requested_nodes": requested_nodes,
            "available_nodes": [],
            "missing_nodes": requested_nodes,
            "edges": [],
            "retrieval_mode": "not_applicable",
            "note": f"Verb `{parsed.verb}` does not require graph retrieval.",
        }

    requested_nodes = dedupe_preserve(parsed.node_ids)
    result = _build_local_graph_toy(requested_nodes, options=options)
    result["provider"] = "toy_graph"
    return result


async def calculate_verb_result(
    parsed: ParsedCAPRequest,
    *,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    del options
    if parsed.verb in STATIC_VERB_CALCULATORS:
        return _calculate_static_result(parsed)
    return _calculate_verb_result_toy(parsed)


def _build_local_graph_toy(
    requested_nodes: list[str],
    *,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    available_set = {node_id for node_id in requested_nodes if is_known_node(node_id)}
    missing_nodes = [node_id for node_id in requested_nodes if not is_known_node(node_id)]

    graph_ops_cfg = (
        options.get("graph_operations", {})
        if isinstance(options, dict) and isinstance(options.get("graph_operations"), dict)
        else {}
    )
    local_graph_cfg = (
        graph_ops_cfg.get("local_graph", {})
        if isinstance(graph_ops_cfg.get("local_graph"), dict)
        else {}
    )
    max_hops = coerce_int(local_graph_cfg.get("max_hops"), default=1, minimum=0)
    max_nodes = coerce_int(local_graph_cfg.get("max_nodes"), default=64, minimum=1)

    frontier: list[tuple[str, int]] = [(node_id, 0) for node_id in available_set]
    seen = set(available_set)
    truncated = False
    while frontier:
        current, depth = frontier.pop()
        if depth >= max_hops:
            continue
        parents = toy_graph.neighbors(current, "parents")
        children = toy_graph.neighbors(current, "children")
        for candidate in parents + children:
            if candidate in seen or not is_known_node(candidate):
                continue
            if len(seen) >= max_nodes:
                truncated = True
                continue
            seen.add(candidate)
            available_set.add(candidate)
            frontier.append((candidate, depth + 1))

    available_nodes = sorted(available_set)
    available_node_rows = [
        {
            "node_id": node_id,
            "node_type": toy_graph.NODE_TYPES.get(node_id, "unknown"),
            "domain": toy_graph.NODE_DOMAINS.get(node_id, "unknown"),
        }
        for node_id in available_nodes
    ]

    edge_rows = [
        {
            "source": edge.source,
            "target": edge.target,
            "weight": edge.weight,
            "relation": "directed_causal_link",
        }
        for edge in toy_graph.EDGES
        if edge.source in available_set and edge.target in available_set
    ]

    return {
        "requested_nodes": requested_nodes,
        "available_nodes": available_node_rows,
        "missing_nodes": missing_nodes,
        "edges": edge_rows,
        "truncated": truncated,
        "retrieval_mode": "local_bounded",
    }


def _calculate_verb_result_toy(parsed: ParsedCAPRequest) -> dict[str, Any]:
    verb = parsed.verb
    params = parsed.params

    if verb not in PROCEDURAL_VERB_CALCULATORS:
        return {
            "status": "not_implemented_in_demo",
            "message": f"No calculator is wired for verb `{verb}` in this demo extension.",
            "supported_verbs": sorted(SUPPORTED_VERB_CALCULATORS),
        }

    if verb == "observe.predict":
        target_node = require_node_param(params, "target_node")
        if not is_known_node(target_node):
            return missing_nodes_result(target_node)
        return {
            "target_node": target_node,
            "prediction": toy_graph.compute_prediction(target_node),
            "drivers": toy_graph.strongest_drivers(target_node),
        }

    if verb == "intervene.do":
        treatment = require_node_param(params, "treatment_node")
        outcome = require_node_param(params, "outcome_node")
        treatment_value = coerce_float(params.get("treatment_value"), default=1.0)
        unknown = [node for node in (treatment, outcome) if not is_known_node(node)]
        if unknown:
            return missing_nodes_result(*unknown)
        effect = round(toy_graph.total_path_effect(treatment, outcome) * treatment_value, 4)
        return {
            "treatment_node": treatment,
            "treatment_value": treatment_value,
            "outcome_node": outcome,
            "effect": effect,
        }

    if verb == "graph.neighbors":
        node_id = require_node_param(params, "node_id")
        if not is_known_node(node_id):
            return missing_nodes_result(node_id)
        scope = params.get("scope", "parents")
        if scope not in {"parents", "children"}:
            return {
                "status": "invalid_scope",
                "message": f"`scope` must be `parents` or `children`, got `{scope}`.",
            }
        max_neighbors = coerce_int(params.get("max_neighbors"), default=10, minimum=1)
        neighbor_ids = toy_graph.neighbors(node_id, scope)
        role = "parent" if scope == "parents" else "child"
        neighbors = [
            {
                "node_id": candidate,
                "tau": None,
                "roles": [role],
            }
            for candidate in neighbor_ids[:max_neighbors]
        ]
        return {
            "node_id": node_id,
            "scope": scope,
            "neighbors": neighbors,
            "total_candidate_count": len(neighbor_ids),
            "truncated": len(neighbor_ids) > max_neighbors,
        }

    if verb == "graph.markov_blanket":
        node_id = require_node_param(params, "node_id")
        if not is_known_node(node_id):
            return missing_nodes_result(node_id)
        max_neighbors = coerce_int(params.get("max_neighbors"), default=30, minimum=1)
        blanket = _toy_markov_blanket(node_id=node_id, max_neighbors=max_neighbors)
        return {
            "node_id": node_id,
            "neighbors": blanket["neighbors"],
            "total_candidate_count": blanket["total_candidate_count"],
            "truncated": blanket["truncated"],
        }

    if verb == "graph.paths":
        source = require_node_param(params, "source_node_id")
        target = require_node_param(params, "target_node_id")
        unknown = [node for node in (source, target) if not is_known_node(node)]
        if unknown:
            return missing_nodes_result(*unknown)
        max_paths = coerce_int(params.get("max_paths"), default=5, minimum=1)
        raw_paths = toy_graph.find_paths(source, target, max_paths)
        paths = [
            {
                "distance": max(0, len(path) - 1),
                "accumulated_tau": None,
                "node_ids": path,
                "edges": [
                    {
                        "from_node_id": path[index],
                        "to_node_id": path[index + 1],
                        "tau": None,
                    }
                    for index in range(len(path) - 1)
                ],
            }
            for path in raw_paths
        ]
        return {
            "source_node_id": source,
            "target_node_id": target,
            "connected": bool(paths),
            "path_count": len(paths),
            "paths": paths,
        }

    if verb == "extensions.example.connectivity_report":
        source = require_node_param(params, "source_node_id")
        target = require_node_param(params, "target_node_id")
        max_paths = coerce_int(params.get("max_paths"), default=20, minimum=1)
        return toy_graph.connectivity_report(source, target, max_paths=max_paths)

    if verb == "extensions.example.path_contribution_report":
        source = require_node_param(params, "source_node_id")
        target = require_node_param(params, "target_node_id")
        max_paths = coerce_int(params.get("max_paths"), default=20, minimum=1)
        return toy_graph.path_contribution_report(source, target, max_paths=max_paths)

    if verb == "extensions.example.market_impact":
        target_node = params.get("target_node")
        if not isinstance(target_node, str) or not target_node:
            return {
                "status": "invalid_request",
                "message": "`params.target_node` is required and must be a string.",
            }
        intervention_delta = coerce_float(params.get("intervention_delta"), default=1.0)
        min_effect_threshold = max(
            0.0,
            coerce_float(params.get("min_effect_threshold"), default=0.0001),
        )
        return toy_graph.market_impact_report(
            target_node,
            intervention_delta,
            min_effect_threshold=min_effect_threshold,
        )

    if verb == "extensions.example.node_systemic_risk":
        node_id = params.get("node_id")
        if not isinstance(node_id, str) or not node_id:
            return {
                "status": "invalid_request",
                "message": "`params.node_id` is required and must be a string.",
            }
        stress_delta = coerce_float(params.get("stress_delta"), default=1.0)
        min_effect_threshold = max(
            0.0,
            coerce_float(params.get("min_effect_threshold"), default=0.0001),
        )
        return toy_graph.node_systemic_risk_report(
            node_id,
            stress_delta,
            min_effect_threshold=min_effect_threshold,
        )

    if verb == "extensions.example.multi_intervention_impact":
        raw_interventions = params.get("interventions")
        if not isinstance(raw_interventions, list) or not raw_interventions:
            return {
                "status": "invalid_request",
                "message": "`params.interventions` is required and must be a non-empty array.",
            }
        interventions: list[dict[str, Any]] = []
        for index, item in enumerate(raw_interventions):
            if not isinstance(item, dict):
                return {
                    "status": "invalid_request",
                    "message": (
                        f"`params.interventions[{index}]` must be an object with "
                        "`target_node` and optional `intervention_delta`."
                    ),
                }
            target_node = item.get("target_node")
            if not isinstance(target_node, str) or not target_node:
                return {
                    "status": "invalid_request",
                    "message": (
                        f"`params.interventions[{index}].target_node` is required "
                        "and must be a string."
                    ),
                }
            interventions.append(
                {
                    "target_node": target_node,
                    "intervention_delta": coerce_float(item.get("intervention_delta"), default=1.0),
                }
            )
        min_effect_threshold = max(
            0.0,
            coerce_float(params.get("min_effect_threshold"), default=0.0001),
        )
        return toy_graph.multi_intervention_impact_report(
            interventions,
            min_effect_threshold=min_effect_threshold,
        )

    if verb == "extensions.example.intervention_ranking":
        outcome_node = params.get("outcome_node")
        if not isinstance(outcome_node, str) or not outcome_node:
            return {
                "status": "invalid_request",
                "message": "`params.outcome_node` is required and must be a string.",
            }
        intervention_delta = coerce_float(params.get("intervention_delta"), default=1.0)
        top_k = coerce_int(params.get("top_k"), default=5, minimum=1)
        min_effect_threshold = max(
            0.0,
            coerce_float(params.get("min_effect_threshold"), default=0.0001),
        )
        raw_candidates = params.get("candidate_nodes")
        if raw_candidates is None:
            candidate_nodes = None
        elif isinstance(raw_candidates, str):
            candidate_nodes = [raw_candidates]
        elif isinstance(raw_candidates, list):
            candidate_nodes = [item for item in raw_candidates if isinstance(item, str)]
        else:
            return {
                "status": "invalid_request",
                "message": "`params.candidate_nodes` must be a string array when provided.",
            }
        return toy_graph.intervention_ranking_report(
            outcome_node,
            intervention_delta,
            candidate_nodes=candidate_nodes,
            top_k=top_k,
            min_effect_threshold=min_effect_threshold,
        )

    if verb == "traverse.parents":
        node_id = require_node_param(params, "node_id")
        if not is_known_node(node_id):
            return missing_nodes_result(node_id)
        top_k = coerce_int(params.get("top_k"), default=5, minimum=1)
        parents = toy_graph.neighbors(node_id, "parents")
        return {
            "node_id": node_id,
            "direction": "parents",
            "nodes": parents[:top_k],
        }

    node_id = require_node_param(params, "node_id")
    if not is_known_node(node_id):
        return missing_nodes_result(node_id)
    top_k = coerce_int(params.get("top_k"), default=5, minimum=1)
    children = toy_graph.neighbors(node_id, "children")
    return {
        "node_id": node_id,
        "direction": "children",
        "nodes": children[:top_k],
    }


def _calculate_static_result(parsed: ParsedCAPRequest) -> dict[str, Any]:
    verb = parsed.verb
    params = parsed.params
    if verb == "meta.capabilities":
        return _meta_capabilities_result()
    if verb == "meta.methods":
        detail = params.get("detail", "compact")
        return _meta_methods_result(detail=str(detail))
    if verb == "extensions.example.dataset_density":
        return _dataset_density_result()
    if verb == "extensions.example.verb_catalog":
        detail = params.get("detail", "full")
        include_examples = bool(params.get("include_examples", True))
        return _verb_catalog_result(detail=str(detail), include_examples=include_examples)
    return _dataset_profile_result()


def _meta_capabilities_result() -> dict[str, Any]:
    method_catalog = _method_catalog()
    core = sorted(
        method["verb"]
        for method in method_catalog
        if method["category"] == "core"
    )
    convenience = sorted(
        method["verb"]
        for method in method_catalog
        if method["category"] == "convenience"
    )
    extension_groups: dict[str, list[str]] = {}
    for method in method_catalog:
        if method["category"] != "extension":
            continue
        namespace = method["verb"].split(".", maxsplit=2)[1]
        extension_groups.setdefault(namespace, []).append(method["verb"])

    return {
        "name": "CAP Example Server",
        "cap_version": "0.2.2",
        "conformance_level": 2,
        "supported_verbs": {
            "core": core,
            "convenience": convenience,
            "extension": {k: sorted(v) for k, v in extension_groups.items()},
        },
        "graph_metadata": {
            "graph_id": toy_graph.GRAPH_ID,
            "graph_version": toy_graph.GRAPH_VERSION,
            "synthetic": True,
            "node_count": len(toy_graph.NODE_BASELINES),
            "edge_count": len(toy_graph.EDGES),
        },
    }


def _meta_methods_result(*, detail: str = "compact") -> dict[str, Any]:
    methods = _method_catalog()
    show_full = detail == "full"
    rows = []
    for method in methods:
        row = {
            "verb": method["verb"],
            "category": method["category"],
            "runtime_owner": method["runtime_owner"],
            "requires_graph": method["requires_graph"],
        }
        if show_full:
            row["input_requirements"] = method["input_requirements"]
            row["primary_functions"] = method["primary_functions"]
            row["description"] = method["description"]
        rows.append(row)
    return {"methods": rows}


def _dataset_profile_result() -> dict[str, Any]:
    return {
        "graph_id": toy_graph.GRAPH_ID,
        "graph_version": toy_graph.GRAPH_VERSION,
        "synthetic": True,
        "node_count": len(toy_graph.NODE_BASELINES),
        "edge_count": len(toy_graph.EDGES),
    }


def _dataset_density_result() -> dict[str, Any]:
    metrics = toy_graph.dataset_density_metrics()
    return {
        "graph_id": toy_graph.GRAPH_ID,
        "graph_version": toy_graph.GRAPH_VERSION,
        "synthetic": True,
        **metrics,
    }


def _verb_catalog_result(*, detail: str = "full", include_examples: bool = True) -> dict[str, Any]:
    methods = _meta_methods_result(detail=detail).get("methods", [])
    if include_examples:
        for method in methods:
            verb = method.get("verb")
            method["example"] = {"cap_version": "0.2.2", "verb": verb}
    core = sorted(
        method["verb"]
        for method in methods
        if method.get("category") == "core"
    )
    convenience = sorted(
        method["verb"]
        for method in methods
        if method.get("category") == "convenience"
    )
    extensions: dict[str, list[str]] = {}
    for method in methods:
        verb = method.get("verb", "")
        if not isinstance(verb, str) or not verb.startswith("extensions."):
            continue
        namespace = verb.split(".", maxsplit=2)[1]
        extensions.setdefault(namespace, []).append(verb)

    return {
        "summary": {
            "core_count": len(core),
            "convenience_count": len(convenience),
            "extension_count": sum(len(values) for values in extensions.values()),
        },
        "supported_verbs": {
            "core": core,
            "convenience": convenience,
            "extensions": {key: sorted(values) for key, values in extensions.items()},
        },
        "methods": methods,
    }


def _method_catalog() -> list[dict[str, Any]]:
    cap_map = load_cap_function_map()
    verb_map = cap_map.get("cap_verb_function_map", {})
    rows = []
    for verb, spec in verb_map.items():
        category = _verb_category(verb)
        primary = spec.get("primary_functions") or spec.get("functions") or []
        rows.append(
            {
                "verb": verb,
                "category": category,
                "runtime_owner": spec.get("runtime_owner", "unknown"),
                "input_requirements": spec.get("input_requirements", []),
                "primary_functions": primary,
                "requires_graph": category in {"core", "convenience"} and verb not in {"meta.capabilities", "meta.methods"},
                "description": _verb_description(verb),
            }
        )
    return sorted(rows, key=lambda item: (item["category"], item["verb"]))


def _verb_category(verb: str) -> str:
    if verb in {"traverse.parents", "traverse.children"}:
        return "convenience"
    if verb.startswith("extensions."):
        return "extension"
    return "core"


def _verb_description(verb: str) -> str:
    descriptions = {
        "meta.capabilities": "Capability discovery for mounted CAP surface.",
        "meta.methods": "Method descriptor discovery and verb contracts.",
        "observe.predict": "Observational prediction for a target node.",
        "intervene.do": "Intervention simulation from treatment to outcome.",
        "graph.neighbors": "Immediate structural parents or children of a node.",
        "graph.markov_blanket": "Parents, children, and spouse nodes of a focal node.",
        "graph.paths": "Shortest directed paths between source and target.",
        "traverse.parents": "Convenience traversal for parent nodes.",
        "traverse.children": "Convenience traversal for child nodes.",
        "extensions.example.dataset_profile": "Synthetic dataset profile metadata.",
        "extensions.example.connectivity_report": (
            "Connectivity summary between two nodes with shortest/longest/all paths."
        ),
        "extensions.example.path_contribution_report": (
            "Path-level effect decomposition between source and target with contribution shares."
        ),
        "extensions.example.market_impact": (
            "Intervention propagation summary with affected node counts and aggregate market impact level."
        ),
        "extensions.example.node_systemic_risk": (
            "Node-level systemic risk profile under stress with propagation and concentration metrics."
        ),
        "extensions.example.multi_intervention_impact": (
            "Aggregate market impact from multiple simultaneous interventions with per-intervention breakdown."
        ),
        "extensions.example.intervention_ranking": (
            "Rank intervention candidates for an outcome by effect size and aggregate impact."
        ),
        "extensions.example.dataset_density": (
            "Synthetic dataset graph statistics including density, sparsity, and degree summaries."
        ),
        "extensions.example.verb_catalog": "Usage-oriented catalog with method descriptors and examples.",
        "extensions.market.parse_request": "Parse embedded CAP request and return mapped function plan.",
        "extensions.market.batch_execute": "Execute multiple embedded CAP requests via staged market pipeline.",
        "extensions.market.interpret_request": "Staged parse/graph/calc/postprocess/analysis for one embedded CAP request.",
    }
    return descriptions.get(verb, "Verb descriptor not documented in this demo.")


def _toy_markov_blanket(*, node_id: str, max_neighbors: int) -> dict[str, Any]:
    parents = toy_graph.neighbors(node_id, "parents")
    children = toy_graph.neighbors(node_id, "children")
    spouse_set: set[str] = set()
    for child in children:
        spouse_set.update(toy_graph.neighbors(child, "parents"))
    spouse_set.discard(node_id)

    flattened: list[dict[str, Any]] = []
    flattened.extend({"node_id": parent, "tau": None, "roles": ["parent"]} for parent in parents)
    flattened.extend({"node_id": child, "tau": None, "roles": ["child"]} for child in children)
    flattened.extend(
        {
            "node_id": spouse,
            "tau": None,
            "roles": ["spouse"],
        }
        for spouse in sorted(spouse_set)
    )
    total = len(flattened)
    return {
        "neighbors": flattened[:max_neighbors],
        "total_candidate_count": total,
        "truncated": total > max_neighbors,
    }
