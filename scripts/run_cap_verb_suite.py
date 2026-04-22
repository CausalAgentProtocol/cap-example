#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx


@dataclass
class Case:
    name: str
    category: str
    top_level_verb: str
    payload: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Send a full CAP verb suite to a running cap-example server and "
            "write detailed JSON/Markdown reports."
        )
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000",
        help="Base URL of running server (default: http://127.0.0.1:8000)",
    )
    parser.add_argument(
        "--market-node-id",
        default="demand",
        help="Focal node used in pipeline graph requests (default: demand).",
    )
    parser.add_argument(
        "--market-source-node-id",
        default="marketing_spend",
        help="Source node for pipeline graph.paths/intervene tests (default: marketing_spend).",
    )
    parser.add_argument(
        "--market-target-node-id",
        default="revenue",
        help="Target node for pipeline graph.paths/intervene tests (default: revenue).",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="Per-request timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--output-dir",
        default="reports",
        help="Directory where report files are written (default: reports).",
    )
    parser.add_argument(
        "--print-responses",
        action="store_true",
        help="Print each full response JSON to stdout while running.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_url = args.base_url.rstrip("/")
    endpoint = f"{base_url}/cap"
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    cases = build_cases(args)
    print(f"[suite] endpoint={endpoint}")
    print(f"[suite] cases={len(cases)}")

    results: list[dict[str, Any]] = []
    started = time.perf_counter()
    with httpx.Client(timeout=args.timeout_seconds) as client:
        for index, case in enumerate(cases, start=1):
            print(f"[{index:02d}/{len(cases):02d}] {case.name} ({case.top_level_verb})")
            result = run_case(client=client, endpoint=endpoint, case=case)
            results.append(result)
            status_line = (
                f"  http={result['http_status']} request_ok={result['request_ok']} "
                f"functional_ok={result['functional_ok']} duration_ms={result['duration_ms']}"
            )
            print(status_line)
            if not result["functional_ok"] and result.get("failure_reason"):
                print(f"  failure_reason={result['failure_reason']}")
                for detail in result.get("failure_details", []):
                    print(f"  failure_detail={detail}")
            if args.print_responses and result.get("response_json") is not None:
                print(json.dumps(result["response_json"], indent=2, sort_keys=True))

    total_duration_ms = int((time.perf_counter() - started) * 1000)
    report = build_report(
        run_id=run_id,
        base_url=base_url,
        total_duration_ms=total_duration_ms,
        results=results,
    )
    json_path, md_path = write_reports(report=report, output_dir=Path(args.output_dir))
    print(f"[suite] JSON report: {json_path}")
    print(f"[suite] Markdown report: {md_path}")


def build_cases(args: argparse.Namespace) -> list[Case]:
    # Direct CAP surface requests (core/convenience/extensions on /cap)
    direct_cases: list[Case] = [
        _case(
            "direct-meta-capabilities",
            "direct",
            "meta.capabilities",
            {"verb": "meta.capabilities"},
        ),
        _case(
            "direct-meta-methods-compact",
            "direct",
            "meta.methods",
            {"verb": "meta.methods", "params": {"detail": "compact"}},
        ),
        _case(
            "direct-meta-methods-full",
            "direct",
            "meta.methods",
            {"verb": "meta.methods", "params": {"detail": "full"}},
        ),
        _case(
            "direct-observe-predict",
            "direct",
            "observe.predict",
            {"verb": "observe.predict", "params": {"target_node": "revenue"}},
        ),
        _case(
            "direct-intervene-do",
            "direct",
            "intervene.do",
            {
                "verb": "intervene.do",
                "params": {
                    "treatment_node": "marketing_spend",
                    "treatment_value": 2.0,
                    "outcome_node": "revenue",
                },
            },
        ),
        _case(
            "direct-graph-neighbors",
            "direct",
            "graph.neighbors",
            {
                "verb": "graph.neighbors",
                "params": {"node_id": "demand", "scope": "parents", "max_neighbors": 5},
            },
        ),
        _case(
            "direct-graph-markov-blanket",
            "direct",
            "graph.markov_blanket",
            {"verb": "graph.markov_blanket", "params": {"node_id": "demand", "max_neighbors": 20}},
        ),
        _case(
            "direct-graph-paths",
            "direct",
            "graph.paths",
            {
                "verb": "graph.paths",
                "params": {
                    "source_node_id": "marketing_spend",
                    "target_node_id": "revenue",
                    "max_paths": 5,
                    "max_depth": 10,
                    "directed": True,
                },
            },
        ),
        _case(
            "direct-traverse-parents",
            "direct",
            "traverse.parents",
            {"verb": "traverse.parents", "params": {"node_id": "demand", "top_k": 5}},
        ),
        _case(
            "direct-traverse-children",
            "direct",
            "traverse.children",
            {"verb": "traverse.children", "params": {"node_id": "marketing_spend", "top_k": 5}},
        ),
        _case(
            "direct-extension-dataset-profile",
            "direct",
            "extensions.example.dataset_profile",
            {"verb": "extensions.example.dataset_profile"},
        ),
        _case(
            "direct-extension-dataset-density",
            "direct",
            "extensions.example.dataset_density",
            {"verb": "extensions.example.dataset_density"},
        ),
        _case(
            "direct-extension-connectivity-report",
            "direct",
            "extensions.example.connectivity_report",
            {
                "verb": "extensions.example.connectivity_report",
                "params": {
                    "source_node_id": "product_quality",
                    "target_node_id": "revenue",
                    "max_paths": 10,
                },
            },
        ),
        _case(
            "direct-extension-path-contribution-report",
            "direct",
            "extensions.example.path_contribution_report",
            {
                "verb": "extensions.example.path_contribution_report",
                "params": {
                    "source_node_id": "product_quality",
                    "target_node_id": args.market_target_node_id,
                    "max_paths": 10,
                },
            },
        ),
        _case(
            "direct-extension-market-impact",
            "direct",
            "extensions.example.market_impact",
            {
                "verb": "extensions.example.market_impact",
                "params": {
                    "target_node": args.market_source_node_id,
                    "intervention_delta": 1.0,
                    "min_effect_threshold": 0.0001,
                },
            },
        ),
        _case(
            "direct-extension-node-systemic-risk",
            "direct",
            "extensions.example.node_systemic_risk",
            {
                "verb": "extensions.example.node_systemic_risk",
                "params": {
                    "node_id": "product_quality",
                    "stress_delta": 1.0,
                    "min_effect_threshold": 0.0001,
                },
            },
        ),
        _case(
            "direct-extension-multi-intervention-impact",
            "direct",
            "extensions.example.multi_intervention_impact",
            {
                "verb": "extensions.example.multi_intervention_impact",
                "params": {
                    "interventions": [
                        {
                            "target_node": args.market_source_node_id,
                            "intervention_delta": 1.0,
                        },
                        {
                            "target_node": "product_quality",
                            "intervention_delta": 1.0,
                        },
                    ],
                    "min_effect_threshold": 0.0001,
                },
            },
        ),
        _case(
            "direct-extension-intervention-ranking",
            "direct",
            "extensions.example.intervention_ranking",
            {
                "verb": "extensions.example.intervention_ranking",
                "params": {
                    "outcome_node": args.market_target_node_id,
                    "intervention_delta": 1.0,
                    "candidate_nodes": [
                        args.market_source_node_id,
                        "product_quality",
                        "demand",
                    ],
                    "top_k": 3,
                    "min_effect_threshold": 0.0001,
                },
            },
        ),
        _case(
            "direct-extension-node-criticality-ranking",
            "direct",
            "extensions.example.node_criticality_ranking",
            {
                "verb": "extensions.example.node_criticality_ranking",
                "params": {
                    "candidate_nodes": [
                        args.market_source_node_id,
                        "product_quality",
                        "demand",
                        "retention",
                    ],
                    "stress_delta": 1.0,
                    "top_k": 3,
                    "min_effect_threshold": 0.0001,
                },
            },
        ),
        _case(
            "direct-extension-edge-criticality-ranking",
            "direct",
            "extensions.example.edge_criticality_ranking",
            {
                "verb": "extensions.example.edge_criticality_ranking",
                "params": {"top_k": 3},
            },
        ),
        _case(
            "direct-extension-goal-seek-intervention",
            "direct",
            "extensions.example.goal_seek_intervention",
            {
                "verb": "extensions.example.goal_seek_intervention",
                "params": {
                    "outcome_node": args.market_target_node_id,
                    "target_outcome_change": 3.0,
                    "candidate_nodes": [
                        args.market_source_node_id,
                        "product_quality",
                        "demand",
                    ],
                    "max_plans": 3,
                    "min_effect_threshold": 0.0001,
                },
            },
        ),
        _case(
            "direct-extension-budgeted-intervention-optimizer",
            "direct",
            "extensions.example.budgeted_intervention_optimizer",
            {
                "verb": "extensions.example.budgeted_intervention_optimizer",
                "params": {
                    "outcome_node": args.market_target_node_id,
                    "budget": 2.0,
                    "objective": "increase",
                    "candidate_nodes": [
                        args.market_source_node_id,
                        "product_quality",
                        "demand",
                    ],
                    "max_allocations": 2,
                    "min_effect_threshold": 0.0001,
                },
            },
        ),
        _case(
            "direct-extension-pareto-intervention-frontier",
            "direct",
            "extensions.example.pareto_intervention_frontier",
            {
                "verb": "extensions.example.pareto_intervention_frontier",
                "params": {
                    "outcome_node": args.market_target_node_id,
                    "intervention_delta": 1.0,
                    "objective": "increase",
                    "candidate_nodes": [
                        args.market_source_node_id,
                        "product_quality",
                        "demand",
                        "retention",
                    ],
                    "min_effect_threshold": 0.0001,
                },
            },
        ),
        _case(
            "direct-extension-scenario-compare",
            "direct",
            "extensions.example.scenario_compare",
            {
                "verb": "extensions.example.scenario_compare",
                "params": {
                    "outcome_node": args.market_target_node_id,
                    "scenarios": [
                        {
                            "name": "growth_push",
                            "interventions": [
                                {
                                    "target_node": args.market_source_node_id,
                                    "intervention_delta": 1.5,
                                },
                                {
                                    "target_node": "product_quality",
                                    "intervention_delta": 0.5,
                                },
                            ],
                        },
                        {
                            "name": "quality_focus",
                            "interventions": [
                                {"target_node": "product_quality", "intervention_delta": 1.5},
                            ],
                        },
                    ],
                    "min_effect_threshold": 0.0001,
                },
            },
        ),
        _case(
            "direct-extension-shock-cascade-simulation",
            "direct",
            "extensions.example.shock_cascade_simulation",
            {
                "verb": "extensions.example.shock_cascade_simulation",
                "params": {
                    "target_node": "product_quality",
                    "shock_delta": 1.0,
                    "steps": 3,
                    "damping": 0.6,
                    "min_effect_threshold": 0.0001,
                },
            },
        ),
        _case(
            "direct-extension-resilience-report",
            "direct",
            "extensions.example.resilience_report",
            {
                "verb": "extensions.example.resilience_report",
                "params": {
                    "top_k": 3,
                    "min_effect_threshold": 0.0001,
                },
            },
        ),
        _case(
            "direct-extension-target-vulnerability-report",
            "direct",
            "extensions.example.target_vulnerability_report",
            {
                "verb": "extensions.example.target_vulnerability_report",
                "params": {
                    "target_node": args.market_target_node_id,
                    "shock_delta": 1.0,
                    "candidate_sources": [
                        args.market_source_node_id,
                        "product_quality",
                        "demand",
                        "retention",
                    ],
                    "top_k": 3,
                    "min_effect_threshold": 0.0001,
                },
            },
        ),
        _case(
            "direct-extension-bottleneck-report",
            "direct",
            "extensions.example.bottleneck_report",
            {
                "verb": "extensions.example.bottleneck_report",
                "params": {
                    "top_k": 3,
                    "max_paths_per_pair": 20,
                },
            },
        ),
        _case(
            "direct-extension-influence-matrix",
            "direct",
            "extensions.example.influence_matrix",
            {
                "verb": "extensions.example.influence_matrix",
                "params": {
                    "node_ids": [
                        "marketing_spend",
                        "product_quality",
                        "demand",
                        "retention",
                        args.market_target_node_id,
                    ]
                },
            },
        ),
        _case(
            "direct-extension-intervention-battle",
            "direct",
            "extensions.example.intervention_battle",
            {
                "verb": "extensions.example.intervention_battle",
                "params": {
                    "outcome_node": args.market_target_node_id,
                    "plan_a": {
                        "name": "acquisition_heavy",
                        "interventions": [
                            {"target_node": args.market_source_node_id, "intervention_delta": 1.5}
                        ],
                    },
                    "plan_b": {
                        "name": "product_heavy",
                        "interventions": [
                            {"target_node": "product_quality", "intervention_delta": 1.0}
                        ],
                    },
                    "min_effect_threshold": 0.0001,
                    "disruption_penalty": 1.0,
                },
            },
        ),
        _case(
            "direct-extension-verb-catalog",
            "direct",
            "extensions.example.verb_catalog",
            {"verb": "extensions.example.verb_catalog", "params": {"detail": "compact"}},
        ),
        _case(
            "direct-extension-market-parse-request",
            "direct",
            "extensions.market.parse_request",
            {
                "verb": "extensions.market.parse_request",
                "params": {
                    "request": {
                        "cap_version": "0.2.2",
                        "verb": "graph.neighbors",
                        "params": {"node_id": args.market_node_id, "scope": "parents"},
                    }
                },
            },
        ),
        _case(
            "direct-extension-market-batch-execute",
            "direct",
            "extensions.market.batch_execute",
            {
                "verb": "extensions.market.batch_execute",
                "params": {
                    "requests": [
                        {
                            "cap_version": "0.2.2",
                            "verb": "observe.predict",
                            "params": {"target_node": args.market_target_node_id},
                        },
                        {
                            "cap_version": "0.2.2",
                            "verb": "graph.neighbors",
                            "params": {
                                "node_id": args.market_node_id,
                                "scope": "parents",
                                "max_neighbors": 8,
                            },
                        },
                    ],
                    "stop_on_error": False,
                },
            },
        ),
    ]

    pipeline_cases: list[Case] = []
    pipeline_embedded_cases = [
        ("meta.capabilities", {}),
        ("meta.methods", {"detail": "compact"}),
        ("observe.predict", {"target_node": args.market_target_node_id}),
        (
            "intervene.do",
            {
                "treatment_node": args.market_source_node_id,
                "treatment_value": 1.0,
                "outcome_node": args.market_target_node_id,
            },
        ),
        ("graph.neighbors", {"node_id": args.market_node_id, "scope": "parents", "max_neighbors": 8}),
        ("graph.markov_blanket", {"node_id": args.market_node_id, "max_neighbors": 20}),
        (
            "graph.paths",
            {
                "source_node_id": args.market_source_node_id,
                "target_node_id": args.market_target_node_id,
                "max_paths": 5,
                "max_depth": 8,
                "directed": True,
            },
        ),
        ("traverse.parents", {"node_id": args.market_node_id, "top_k": 8}),
        ("traverse.children", {"node_id": args.market_node_id, "top_k": 8}),
        ("extensions.example.dataset_profile", {}),
        (
            "extensions.example.connectivity_report",
            {
                "source_node_id": "product_quality",
                "target_node_id": args.market_target_node_id,
                "max_paths": 10,
            },
        ),
        (
            "extensions.example.market_impact",
            {
                "target_node": args.market_source_node_id,
                "intervention_delta": 1.0,
                "min_effect_threshold": 0.0001,
            },
        ),
        (
            "extensions.example.path_contribution_report",
            {
                "source_node_id": "product_quality",
                "target_node_id": args.market_target_node_id,
                "max_paths": 10,
            },
        ),
        (
            "extensions.example.node_systemic_risk",
            {
                "node_id": "product_quality",
                "stress_delta": 1.0,
                "min_effect_threshold": 0.0001,
            },
        ),
        (
            "extensions.example.multi_intervention_impact",
            {
                "interventions": [
                    {
                        "target_node": args.market_source_node_id,
                        "intervention_delta": 1.0,
                    },
                    {
                        "target_node": "product_quality",
                        "intervention_delta": 1.0,
                    },
                ],
                "min_effect_threshold": 0.0001,
            },
        ),
        (
            "extensions.example.intervention_ranking",
            {
                "outcome_node": args.market_target_node_id,
                "intervention_delta": 1.0,
                "candidate_nodes": [
                    args.market_source_node_id,
                    "product_quality",
                    "demand",
                ],
                "top_k": 3,
                "min_effect_threshold": 0.0001,
            },
        ),
        (
            "extensions.example.node_criticality_ranking",
            {
                "candidate_nodes": [
                    args.market_source_node_id,
                    "product_quality",
                    "demand",
                    "retention",
                ],
                "stress_delta": 1.0,
                "top_k": 3,
                "min_effect_threshold": 0.0001,
            },
        ),
        (
            "extensions.example.edge_criticality_ranking",
            {"top_k": 3},
        ),
        (
            "extensions.example.goal_seek_intervention",
            {
                "outcome_node": args.market_target_node_id,
                "target_outcome_change": 3.0,
                "candidate_nodes": [
                    args.market_source_node_id,
                    "product_quality",
                    "demand",
                ],
                "max_plans": 3,
                "min_effect_threshold": 0.0001,
            },
        ),
        (
            "extensions.example.budgeted_intervention_optimizer",
            {
                "outcome_node": args.market_target_node_id,
                "budget": 2.0,
                "objective": "increase",
                "candidate_nodes": [
                    args.market_source_node_id,
                    "product_quality",
                    "demand",
                ],
                "max_allocations": 2,
                "min_effect_threshold": 0.0001,
            },
        ),
        (
            "extensions.example.pareto_intervention_frontier",
            {
                "outcome_node": args.market_target_node_id,
                "intervention_delta": 1.0,
                "objective": "increase",
                "candidate_nodes": [
                    args.market_source_node_id,
                    "product_quality",
                    "demand",
                    "retention",
                ],
                "min_effect_threshold": 0.0001,
            },
        ),
        (
            "extensions.example.scenario_compare",
            {
                "outcome_node": args.market_target_node_id,
                "scenarios": [
                    {
                        "name": "growth_push",
                        "interventions": [
                            {
                                "target_node": args.market_source_node_id,
                                "intervention_delta": 1.5,
                            },
                            {"target_node": "product_quality", "intervention_delta": 0.5},
                        ],
                    },
                    {
                        "name": "quality_focus",
                        "interventions": [
                            {"target_node": "product_quality", "intervention_delta": 1.5},
                        ],
                    },
                ],
                "min_effect_threshold": 0.0001,
            },
        ),
        (
            "extensions.example.shock_cascade_simulation",
            {
                "target_node": "product_quality",
                "shock_delta": 1.0,
                "steps": 3,
                "damping": 0.6,
                "min_effect_threshold": 0.0001,
            },
        ),
        (
            "extensions.example.resilience_report",
            {"top_k": 3, "min_effect_threshold": 0.0001},
        ),
        (
            "extensions.example.target_vulnerability_report",
            {
                "target_node": args.market_target_node_id,
                "shock_delta": 1.0,
                "candidate_sources": [
                    args.market_source_node_id,
                    "product_quality",
                    "demand",
                    "retention",
                ],
                "top_k": 3,
                "min_effect_threshold": 0.0001,
            },
        ),
        (
            "extensions.example.bottleneck_report",
            {"top_k": 3, "max_paths_per_pair": 20},
        ),
        (
            "extensions.example.influence_matrix",
            {
                "node_ids": [
                    "marketing_spend",
                    "product_quality",
                    "demand",
                    "retention",
                    args.market_target_node_id,
                ]
            },
        ),
        (
            "extensions.example.intervention_battle",
            {
                "outcome_node": args.market_target_node_id,
                "plan_a": {
                    "name": "acquisition_heavy",
                    "interventions": [
                        {"target_node": args.market_source_node_id, "intervention_delta": 1.5}
                    ],
                },
                "plan_b": {
                    "name": "product_heavy",
                    "interventions": [
                        {"target_node": "product_quality", "intervention_delta": 1.0}
                    ],
                },
                "min_effect_threshold": 0.0001,
                "disruption_penalty": 1.0,
            },
        ),
        ("extensions.example.dataset_density", {}),
        ("extensions.example.verb_catalog", {"detail": "compact", "include_examples": False}),
    ]
    for embedded_verb, embedded_params in pipeline_embedded_cases:
        payload: dict[str, Any] = {
            "verb": "extensions.market.interpret_request",
            "params": {
                "request": {
                    "cap_version": "0.2.2",
                    "verb": embedded_verb,
                }
            },
        }
        if embedded_params:
            payload["params"]["request"]["params"] = embedded_params
        pipeline_cases.append(
            _case(
                name=f"pipeline-{embedded_verb.replace('.', '-')}",
                category="pipeline",
                top_level_verb="extensions.market.interpret_request",
                payload=payload,
            )
        )

    return direct_cases + pipeline_cases

def _case(
    name: str,
    category: str,
    top_level_verb: str,
    payload: dict[str, Any],
) -> Case:
    body = {
        "cap_version": "0.2.2",
        "request_id": f"{name}-{int(time.time() * 1000)}",
        **payload,
    }
    return Case(
        name=name,
        category=category,
        top_level_verb=top_level_verb,
        payload=body,
    )


def run_case(*, client: httpx.Client, endpoint: str, case: Case) -> dict[str, Any]:
    started = time.perf_counter()
    response_json: dict[str, Any] | None = None
    response_text: str | None = None
    http_status: int | None = None
    error: str | None = None

    try:
        response = client.post(endpoint, json=case.payload)
        http_status = response.status_code
        response_text = response.text
        if "application/json" in response.headers.get("content-type", ""):
            response_json = response.json()
    except Exception as exc:  # noqa: BLE001
        error = repr(exc)

    duration_ms = int((time.perf_counter() - started) * 1000)
    evaluation = evaluate_response(
        category=case.category,
        http_status=http_status,
        response_json=response_json,
        error=error,
    )
    return {
        "name": case.name,
        "category": case.category,
        "verb": case.top_level_verb,
        "request_payload": redact_sensitive(case.payload),
        "http_status": http_status,
        "duration_ms": duration_ms,
        "error": error,
        "request_ok": evaluation["request_ok"],
        "functional_ok": evaluation["functional_ok"],
        "cap_status": evaluation.get("cap_status"),
        "pipeline_stage_statuses": evaluation.get("pipeline_stage_statuses"),
        "failure_reason": evaluation.get("failure_reason"),
        "failure_details": evaluation.get("failure_details", []),
        "notes": evaluation.get("notes", []),
        "response_json": redact_sensitive(response_json),
        "response_text": response_text,
    }


def evaluate_response(
    *,
    category: str,
    http_status: int | None,
    response_json: dict[str, Any] | None,
    error: str | None,
) -> dict[str, Any]:
    if error:
        return {
            "request_ok": False,
            "functional_ok": False,
            "failure_reason": "request_exception",
            "failure_details": [error],
            "notes": [f"request_exception: {error}"],
        }

    request_ok = http_status == 200 and isinstance(response_json, dict)
    if not request_ok:
        return {
            "request_ok": False,
            "functional_ok": False,
            "failure_reason": "invalid_http_or_json",
            "failure_details": [f"http_status={http_status}"],
            "notes": ["non_200_or_non_json_response"],
        }

    cap_status = response_json.get("status")
    cap_ok = cap_status == "success"
    notes: list[str] = []
    failure_details: list[str] = []
    failure_reason: str | None = None
    if not cap_ok:
        notes.append(f"cap_status={cap_status}")
        failure_reason = "cap_status_not_success"
        failure_details.append(f"cap_status={cap_status}")

    if category != "pipeline":
        failure_details = dedupe_preserve_strings(failure_details)
        return {
            "request_ok": request_ok and cap_ok,
            "functional_ok": request_ok and cap_ok,
            "cap_status": cap_status,
            "failure_reason": failure_reason,
            "failure_details": failure_details,
            "notes": notes,
        }

    stage_statuses = extract_stage_statuses(response_json)
    parse_ok = stage_statuses.get("parse") == "success"
    graph_ok = stage_statuses.get("graph_operations") == "success"
    calc_ok = stage_statuses.get("calculation") == "success"
    post_ok = stage_statuses.get("postprocess") == "success"
    analysis_ok = stage_statuses.get("analysis") == "success"
    functional_ok = cap_ok and parse_ok and graph_ok and calc_ok and post_ok and analysis_ok
    if not functional_ok:
        notes.append(f"pipeline_statuses={stage_statuses}")
        if failure_reason is None:
            failure_reason = "pipeline_stage_failure"
        failure_details.extend(extract_pipeline_failure_details(response_json, stage_statuses))
    failure_details = dedupe_preserve_strings(failure_details)

    return {
        "request_ok": request_ok and cap_ok,
        "functional_ok": functional_ok,
        "cap_status": cap_status,
        "pipeline_stage_statuses": stage_statuses,
        "failure_reason": failure_reason,
        "failure_details": failure_details,
        "notes": notes,
    }


def extract_stage_statuses(body: dict[str, Any]) -> dict[str, str]:
    result = body.get("result", {})
    if not isinstance(result, dict):
        return {}
    stages = result.get("stages", {})
    if not isinstance(stages, dict):
        return {}
    statuses: dict[str, str] = {}
    for stage_name, stage_payload in stages.items():
        if isinstance(stage_payload, dict):
            statuses[stage_name] = str(stage_payload.get("status", "unknown"))
    return statuses


def extract_pipeline_failure_details(
    body: dict[str, Any],
    stage_statuses: dict[str, str],
) -> list[str]:
    details: list[str] = []
    result = body.get("result", {})
    if not isinstance(result, dict):
        return details
    stages = result.get("stages", {})
    if not isinstance(stages, dict):
        return details

    for stage_name, status in stage_statuses.items():
        if status == "success":
            continue
        stage_payload = stages.get(stage_name, {})
        if not isinstance(stage_payload, dict):
            details.append(f"{stage_name}: status={status}")
            continue
        direct_message = stage_payload.get("message")
        data_payload = stage_payload.get("data")
        data_message = None
        if isinstance(data_payload, dict):
            data_message = data_payload.get("message")
        if direct_message:
            details.append(f"{stage_name}: {direct_message}")
        elif data_message:
            details.append(f"{stage_name}: {data_message}")
        else:
            details.append(f"{stage_name}: status={status}")

    # Include top-level calculation message when available.
    calculation = result.get("calculation")
    if isinstance(calculation, dict):
        message = calculation.get("message")
        calc_status = calculation.get("status")
        if message:
            details.append(f"calculation: {message}")
        elif calc_status and calc_status != "success":
            details.append(f"calculation: status={calc_status}")

    return details


def dedupe_preserve_strings(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def build_report(
    *,
    run_id: str,
    base_url: str,
    total_duration_ms: int,
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    total = len(results)
    request_ok_count = sum(1 for item in results if item["request_ok"])
    functional_ok_count = sum(1 for item in results if item["functional_ok"])
    failed_request_cases = [item["name"] for item in results if not item["request_ok"]]
    failed_function_cases = [item["name"] for item in results if not item["functional_ok"]]
    failed_case_reasons = {
        item["name"]: {
            "failure_reason": item.get("failure_reason"),
            "failure_details": item.get("failure_details", []),
        }
        for item in results
        if not item["functional_ok"]
    }

    return {
        "run": {
            "id": run_id,
            "started_at_utc": run_id,
            "base_url": base_url,
            "total_cases": total,
            "total_duration_ms": total_duration_ms,
        },
        "summary": {
            "request_ok_count": request_ok_count,
            "request_ok_rate": _safe_ratio(request_ok_count, total),
            "functional_ok_count": functional_ok_count,
            "functional_ok_rate": _safe_ratio(functional_ok_count, total),
            "failed_request_cases": failed_request_cases,
            "failed_function_cases": failed_function_cases,
            "failed_case_reasons": failed_case_reasons,
        },
        "cases": results,
    }


def _safe_ratio(num: int, den: int) -> float:
    if den <= 0:
        return 0.0
    return round(num / den, 4)


def redact_sensitive(value: Any, *, key_hint: str | None = None) -> Any:
    if isinstance(value, dict):
        return {
            key: redact_sensitive(item, key_hint=key)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_sensitive(item, key_hint=key_hint) for item in value]
    if isinstance(value, str) and is_sensitive_key(key_hint):
        return "***"
    return value


def is_sensitive_key(key: str | None) -> bool:
    if not key:
        return False
    lowered = key.lower()
    return any(token in lowered for token in ("password", "secret", "token"))


def write_reports(*, report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = report["run"]["id"]
    json_path = output_dir / f"cap_verb_suite_{run_id}.json"
    md_path = output_dir / f"cap_verb_suite_{run_id}.md"

    json_path.write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    md_path.write_text(render_markdown_report(report), encoding="utf-8")
    return json_path, md_path


def render_markdown_report(report: dict[str, Any]) -> str:
    run = report["run"]
    summary = report["summary"]
    cases = report["cases"]

    lines = [
        "# CAP Verb Suite Report",
        "",
        f"- Run ID: `{run['id']}`",
        f"- Base URL: `{run['base_url']}`",
        f"- Total cases: `{run['total_cases']}`",
        f"- Total duration: `{run['total_duration_ms']} ms`",
        "",
        "## Summary",
        "",
        f"- Request OK: `{summary['request_ok_count']}/{run['total_cases']}` ({summary['request_ok_rate']})",
        f"- Functional OK: `{summary['functional_ok_count']}/{run['total_cases']}` ({summary['functional_ok_rate']})",
        "",
        "## Case Table",
        "",
        "| Case | Category | Verb | HTTP | Request OK | Functional OK | Failure Reason | Duration (ms) |",
        "| --- | --- | --- | ---: | ---: | ---: | --- | ---: |",
    ]

    for item in cases:
        lines.append(
            "| {name} | {category} | {verb} | {http} | {req_ok} | {func_ok} | {reason} | {dur} |".format(
                name=item["name"],
                category=item["category"],
                verb=item["verb"],
                http=item["http_status"],
                req_ok=str(item["request_ok"]).lower(),
                func_ok=str(item["functional_ok"]).lower(),
                reason=item.get("failure_reason") or "",
                dur=item["duration_ms"],
            )
        )

    lines.append("")
    lines.append("## Case Details")
    lines.append("")

    for item in cases:
        lines.append(f"### {item['name']}")
        lines.append("")
        lines.append(f"- Category: `{item['category']}`")
        lines.append(f"- Verb: `{item['verb']}`")
        lines.append(f"- HTTP status: `{item['http_status']}`")
        lines.append(f"- Request OK: `{item['request_ok']}`")
        lines.append(f"- Functional OK: `{item['functional_ok']}`")
        lines.append(f"- Duration: `{item['duration_ms']} ms`")
        if item.get("failure_reason"):
            lines.append(f"- Failure reason: `{item['failure_reason']}`")
        if item.get("failure_details"):
            lines.append("- Failure details:")
            for detail in item["failure_details"]:
                lines.append(f"  - `{detail}`")
        if item.get("notes"):
            lines.append(f"- Notes: `{'; '.join(item['notes'])}`")
        if item.get("pipeline_stage_statuses"):
            lines.append(
                "- Pipeline stages: "
                + "`"
                + json.dumps(item["pipeline_stage_statuses"], sort_keys=True)
                + "`"
            )
        lines.append("- Request payload:")
        lines.append("```json")
        lines.append(json.dumps(item["request_payload"], indent=2, sort_keys=True))
        lines.append("```")
        lines.append("- Response:")
        lines.append("```json")
        if item.get("response_json") is not None:
            lines.append(json.dumps(item["response_json"], indent=2, sort_keys=True))
        else:
            lines.append(json.dumps({"error": item.get("error"), "response_text": item.get("response_text")}, indent=2))
        lines.append("```")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


if __name__ == "__main__":
    main()
