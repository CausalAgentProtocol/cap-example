from __future__ import annotations

from typing import Any

from example_cap_server.market_pipeline.models import ParsedCAPRequest
from example_cap_server.market_pipeline.utils import dedupe_preserve

NODE_PARAM_KEYS = (
    "target_node",
    "treatment_node",
    "outcome_node",
    "node_id",
    "source_node_id",
    "target_node_id",
)


def parse_cap_request(payload: dict[str, Any]) -> ParsedCAPRequest:
    if not isinstance(payload, dict):
        raise ValueError("`params.request` must be an object.")

    verb = payload.get("verb")
    if not isinstance(verb, str) or not verb:
        raise ValueError("Embedded CAP request must include a non-empty `verb`.")

    raw_params = payload.get("params", {})
    if raw_params is None:
        raw_params = {}
    if not isinstance(raw_params, dict):
        raise ValueError("Embedded CAP request `params` must be an object when provided.")

    return ParsedCAPRequest(
        verb=verb,
        params=raw_params,
        node_ids=extract_node_ids(raw_params),
    )


def extract_node_ids(params: dict[str, Any]) -> list[str]:
    nodes: list[str] = []

    for key in NODE_PARAM_KEYS:
        value = params.get(key)
        if isinstance(value, str):
            nodes.append(value)
        elif isinstance(value, list):
            nodes.extend(item for item in value if isinstance(item, str))

    for key, value in params.items():
        if key in NODE_PARAM_KEYS:
            continue
        if key == "interventions" and isinstance(value, list):
            for row in value:
                if not isinstance(row, dict):
                    continue
                target = row.get("target_node")
                if isinstance(target, str):
                    nodes.append(target)
            continue
        if key in {"plan_a", "plan_b"} and isinstance(value, dict):
            plan_interventions = value.get("interventions")
            if isinstance(plan_interventions, list):
                for row in plan_interventions:
                    if not isinstance(row, dict):
                        continue
                    target = row.get("target_node")
                    if isinstance(target, str):
                        nodes.append(target)
            continue
        if key == "scenarios" and isinstance(value, list):
            for scenario in value:
                if not isinstance(scenario, dict):
                    continue
                scenario_interventions = scenario.get("interventions")
                if not isinstance(scenario_interventions, list):
                    continue
                for row in scenario_interventions:
                    if not isinstance(row, dict):
                        continue
                    target = row.get("target_node")
                    if isinstance(target, str):
                        nodes.append(target)
            continue
        if key == "candidate_sources" and isinstance(value, list):
            nodes.extend(item for item in value if isinstance(item, str))
            continue
        if key.endswith("_node") or key.endswith("_node_id"):
            if isinstance(value, str):
                nodes.append(value)
        elif key.endswith("_nodes") or key.endswith("_node_ids"):
            if isinstance(value, list):
                nodes.extend(item for item in value if isinstance(item, str))

    return dedupe_preserve(nodes)
