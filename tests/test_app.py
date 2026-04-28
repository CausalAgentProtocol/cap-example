from fastapi.testclient import TestClient
import pytest

from example_cap_server.integrations import get_cap_function_plan, load_cap_function_map
from example_cap_server.main import app


client = TestClient(app)

EXAMPLE_VERBS = {
    "extensions.example.dataset_profile",
    "extensions.example.dataset_density",
    "extensions.example.connectivity_report",
    "extensions.example.path_contribution_report",
    "extensions.example.market_impact",
    "extensions.example.node_systemic_risk",
    "extensions.example.multi_intervention_impact",
    "extensions.example.intervention_ranking",
    "extensions.example.node_criticality_ranking",
    "extensions.example.edge_criticality_ranking",
    "extensions.example.goal_seek_intervention",
    "extensions.example.budgeted_intervention_optimizer",
    "extensions.example.pareto_intervention_frontier",
    "extensions.example.scenario_compare",
    "extensions.example.shock_cascade_simulation",
    "extensions.example.resilience_report",
    "extensions.example.target_vulnerability_report",
    "extensions.example.bottleneck_report",
    "extensions.example.influence_matrix",
    "extensions.example.intervention_battle",
    "extensions.example.verb_catalog",
}

NEW_EXTENSION_CASES = [
    (
        "extensions.example.node_criticality_ranking",
        {
            "candidate_nodes": ["marketing_spend", "product_quality", "demand", "retention"],
            "stress_delta": 1.0,
            "top_k": 3,
            "min_effect_threshold": 0.0001,
        },
        "rankings",
    ),
    (
        "extensions.example.edge_criticality_ranking",
        {"top_k": 3},
        "rankings",
    ),
    (
        "extensions.example.goal_seek_intervention",
        {
            "outcome_node": "revenue",
            "target_outcome_change": 3.0,
            "candidate_nodes": ["marketing_spend", "product_quality", "demand"],
            "max_plans": 3,
            "min_effect_threshold": 0.0001,
        },
        "plans",
    ),
    (
        "extensions.example.budgeted_intervention_optimizer",
        {
            "outcome_node": "revenue",
            "budget": 2.0,
            "objective": "increase",
            "candidate_nodes": ["marketing_spend", "product_quality", "demand"],
            "max_allocations": 2,
            "min_effect_threshold": 0.0001,
        },
        "allocations",
    ),
    (
        "extensions.example.pareto_intervention_frontier",
        {
            "outcome_node": "revenue",
            "intervention_delta": 1.0,
            "objective": "increase",
            "candidate_nodes": ["marketing_spend", "product_quality", "demand", "retention"],
            "min_effect_threshold": 0.0001,
        },
        "frontier",
    ),
    (
        "extensions.example.scenario_compare",
        {
            "outcome_node": "revenue",
            "scenarios": [
                {
                    "name": "growth_push",
                    "interventions": [
                        {"target_node": "marketing_spend", "intervention_delta": 1.5},
                        {"target_node": "product_quality", "intervention_delta": 0.5},
                    ],
                },
                {
                    "name": "quality_focus",
                    "interventions": [{"target_node": "product_quality", "intervention_delta": 1.5}],
                },
            ],
            "min_effect_threshold": 0.0001,
        },
        "scenarios",
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
        "steps",
    ),
    (
        "extensions.example.resilience_report",
        {"top_k": 3, "min_effect_threshold": 0.0001},
        "resilience_index",
    ),
    (
        "extensions.example.target_vulnerability_report",
        {
            "target_node": "revenue",
            "shock_delta": 1.0,
            "candidate_sources": ["marketing_spend", "product_quality", "demand", "retention"],
            "top_k": 3,
            "min_effect_threshold": 0.0001,
        },
        "rankings",
    ),
    (
        "extensions.example.bottleneck_report",
        {"top_k": 3, "max_paths_per_pair": 20},
        "top_nodes",
    ),
    (
        "extensions.example.influence_matrix",
        {"node_ids": ["marketing_spend", "product_quality", "demand", "retention", "revenue"]},
        "matrix",
    ),
    (
        "extensions.example.intervention_battle",
        {
            "outcome_node": "revenue",
            "plan_a": {
                "name": "acquisition_heavy",
                "interventions": [{"target_node": "marketing_spend", "intervention_delta": 1.5}],
            },
            "plan_b": {
                "name": "product_heavy",
                "interventions": [{"target_node": "product_quality", "intervention_delta": 1.0}],
            },
            "min_effect_threshold": 0.0001,
            "disruption_penalty": 1.0,
        },
        "winner",
    ),
]


def test_health_endpoint() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_well_known_capability_card_example_only_extensions() -> None:
    response = client.get("/.well-known/cap.json")

    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "CAP Example Server"
    assert body["conformance_level"] == 2
    if "conformance_name" in body:
        assert body["conformance_name"] == "Intervene"
    assert set(body["extensions"].keys()) == {"example"}
    assert set(body["extensions"]["example"]["verbs"]) == EXAMPLE_VERBS


def test_meta_methods_includes_example_extensions_only() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.3.0",
            "request_id": "req-methods",
            "verb": "meta.methods",
            "params": {"detail": "compact"},
        },
    )

    assert response.status_code == 200
    methods = {item["verb"] for item in response.json()["result"]["methods"]}
    assert "meta.capabilities" in methods
    assert "observe.predict" in methods
    assert "traverse.children" in methods
    assert EXAMPLE_VERBS.issubset(methods)
    assert not any(verb.startswith("extensions.market.") for verb in methods)


def test_intervene_do_returns_semantic_fields() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.3.0",
            "request_id": "req-intervene",
            "verb": "intervene.do",
            "params": {
                "treatment_node": "marketing_spend",
                "treatment_value": 2.0,
                "outcome_node": "revenue",
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["result"]["effect"] == 2.4
    assert body["result"]["reasoning_mode"] == "scm_simulation"
    assert body["result"]["identification_status"] == "not_formally_identified"


def test_example_verb_catalog_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.3.0",
            "request_id": "req-verb-catalog",
            "verb": "extensions.example.verb_catalog",
            "params": {
                "detail": "compact",
                "include_examples": False,
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    verbs = {item["verb"] for item in body["result"]["methods"]}
    assert "observe.predict" in verbs
    assert "extensions.example.intervention_battle" in verbs
    assert not any(verb.startswith("extensions.market.") for verb in verbs)


def test_example_dataset_density_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.3.0",
            "request_id": "req-dataset-density",
            "verb": "extensions.example.dataset_density",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    result = body["result"]
    assert result["graph_id"] == "toy-business-dag"
    assert result["node_count"] == 5
    assert result["edge_count"] == 5
    assert result["possible_directed_edges"] == 20
    assert result["missing_directed_edges"] == 15
    assert result["density"] == 0.25
    assert result["sparsity"] == 0.75
    assert result["max_degree"] == 3
    assert result["min_degree"] == 1


def test_example_connectivity_report_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.3.0",
            "request_id": "req-connectivity-report",
            "verb": "extensions.example.connectivity_report",
            "params": {
                "source_node_id": "product_quality",
                "target_node_id": "revenue",
                "max_paths": 10,
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    result = body["result"]
    assert result["connected"] is True
    assert result["path_count"] == 2
    assert result["truncated"] is False
    assert result["shortest_path_length"] == 2
    assert result["longest_path_length"] == 2


def test_example_path_contribution_report_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.3.0",
            "request_id": "req-path-contribution-report",
            "verb": "extensions.example.path_contribution_report",
            "params": {
                "source_node_id": "product_quality",
                "target_node_id": "revenue",
                "max_paths": 10,
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    result = body["result"]
    assert result["connected"] is True
    assert result["path_count"] == 2
    assert result["total_effect"] == 3.0
    assert result["top_contributing_path_effect"] == 2.4


def test_example_market_impact_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.3.0",
            "request_id": "req-market-impact",
            "verb": "extensions.example.market_impact",
            "params": {
                "target_node": "marketing_spend",
                "intervention_delta": 1.0,
                "min_effect_threshold": 0.0001,
            },
        },
    )

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["affected_node_count"] == 3
    assert result["downstream_affected_node_count"] == 2
    assert result["market_change_level"] == 0.0175
    assert result["max_affected_node"] == "revenue"


def test_example_node_systemic_risk_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.3.0",
            "request_id": "req-node-systemic-risk",
            "verb": "extensions.example.node_systemic_risk",
            "params": {
                "node_id": "product_quality",
                "stress_delta": 1.0,
                "min_effect_threshold": 0.0001,
            },
        },
    )

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["node_known"] is True
    assert result["affected_node_count"] == 4
    assert result["systemic_risk_level"] == "critical"


def test_example_multi_intervention_impact_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.3.0",
            "request_id": "req-multi-intervention-impact",
            "verb": "extensions.example.multi_intervention_impact",
            "params": {
                "interventions": [
                    {"target_node": "marketing_spend", "intervention_delta": 1.0},
                    {"target_node": "product_quality", "intervention_delta": 1.0},
                ],
                "min_effect_threshold": 0.0001,
            },
        },
    )

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["intervention_count"] == 2
    assert result["evaluated_intervention_count"] == 2
    assert result["affected_node_count"] == 5


def test_example_intervention_ranking_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.3.0",
            "request_id": "req-intervention-ranking",
            "verb": "extensions.example.intervention_ranking",
            "params": {
                "outcome_node": "revenue",
                "intervention_delta": 1.0,
                "candidate_nodes": [
                    "marketing_spend",
                    "product_quality",
                    "demand",
                    "unknown_node",
                ],
                "top_k": 3,
                "min_effect_threshold": 0.0001,
            },
        },
    )

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["candidate_count"] == 4
    assert result["evaluated_candidate_count"] == 3
    assert result["ranked_candidate_count"] == 3
    assert result["missing_candidate_nodes"] == ["unknown_node"]
    assert [row["candidate_node"] for row in result["rankings"]] == [
        "product_quality",
        "demand",
        "marketing_spend",
    ]


@pytest.mark.parametrize(("verb", "params", "expected_key"), NEW_EXTENSION_CASES)
def test_new_extensions_direct_smoke(
    verb: str,
    params: dict,
    expected_key: str,
) -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.3.0",
            "request_id": f"req-{verb.replace('.', '-')}",
            "verb": verb,
            "params": params,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert expected_key in body["result"]


def test_cap_function_map_has_core_and_example_verbs() -> None:
    cap_map = load_cap_function_map()
    verb_map = cap_map["cap_verb_function_map"]
    assert "observe.predict" in verb_map
    assert "intervene.do" in verb_map
    assert "graph.neighbors" in verb_map
    assert "graph.markov_blanket" in verb_map
    assert "graph.paths" in verb_map
    assert EXAMPLE_VERBS.issubset(set(verb_map.keys()))
    assert "extensions.market.parse_request" not in verb_map
    assert "extensions.market.batch_execute" not in verb_map
    assert "extensions.market.interpret_request" not in verb_map


def test_get_cap_function_plan_returns_mapping_for_verb() -> None:
    plan = get_cap_function_plan("observe.predict")
    assert plan["runtime_owner"] == "cap-example"
    assert "primary_functions" in plan
