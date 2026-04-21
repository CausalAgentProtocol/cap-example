from fastapi.testclient import TestClient

from example_cap_server.integrations import get_cap_function_plan, load_cap_function_map
from example_cap_server.main import app


client = TestClient(app)


def test_health_endpoint() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_well_known_capability_card() -> None:
    response = client.get("/.well-known/cap.json")

    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "CAP Example Server"
    assert body["conformance_level"] == 2
    assert "extensions.example.dataset_profile" in body["extensions"]["example"]["verbs"]
    assert "extensions.example.connectivity_report" in body["extensions"]["example"]["verbs"]
    assert "extensions.example.path_contribution_report" in body["extensions"]["example"]["verbs"]
    assert "extensions.example.market_impact" in body["extensions"]["example"]["verbs"]
    assert "extensions.example.node_systemic_risk" in body["extensions"]["example"]["verbs"]
    assert "extensions.example.multi_intervention_impact" in body["extensions"]["example"]["verbs"]
    assert "extensions.example.intervention_ranking" in body["extensions"]["example"]["verbs"]
    assert "extensions.example.dataset_density" in body["extensions"]["example"]["verbs"]
    assert "extensions.example.verb_catalog" in body["extensions"]["example"]["verbs"]
    assert "extensions.market.parse_request" in body["extensions"]["market"]["verbs"]
    assert "extensions.market.batch_execute" in body["extensions"]["market"]["verbs"]
    assert "extensions.market.interpret_request" in body["extensions"]["market"]["verbs"]


def test_meta_methods_includes_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-methods",
            "verb": "meta.methods",
            "params": {"detail": "compact"},
        },
    )

    assert response.status_code == 200
    methods = {item["verb"] for item in response.json()["result"]["methods"]}
    assert "meta.capabilities" in methods
    assert "extensions.example.dataset_profile" in methods
    assert "extensions.example.connectivity_report" in methods
    assert "extensions.example.path_contribution_report" in methods
    assert "extensions.example.market_impact" in methods
    assert "extensions.example.node_systemic_risk" in methods
    assert "extensions.example.multi_intervention_impact" in methods
    assert "extensions.example.intervention_ranking" in methods
    assert "extensions.example.dataset_density" in methods
    assert "extensions.example.verb_catalog" in methods
    assert "extensions.market.parse_request" in methods
    assert "extensions.market.batch_execute" in methods
    assert "extensions.market.interpret_request" in methods


def test_intervene_do_returns_semantic_fields() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
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


def test_market_interpret_request_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-market-interpret",
            "verb": "extensions.market.interpret_request",
            "params": {
                "request": {
                    "cap_version": "0.2.2",
                    "verb": "graph.neighbors",
                    "params": {
                        "node_id": "demand",
                        "scope": "parents",
                        "max_neighbors": 5,
                    },
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["result"]["parsed_request"]["verb"] == "graph.neighbors"
    assert "demand" in body["result"]["parsed_request"]["node_ids"]
    neighbor_ids = [item["node_id"] for item in body["result"]["calculation"]["neighbors"]]
    assert neighbor_ids == ["marketing_spend", "product_quality"]
    assert body["result"]["graph_excerpt"]["provider"] == "toy_graph"
    assert set(body["result"]["stages"].keys()) == {
        "parse",
        "graph_operations",
        "calculation",
        "postprocess",
        "analysis",
    }


def test_example_verb_catalog_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
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
    verbs = {
        item["verb"]
        for item in body["result"]["methods"]
    }
    assert "observe.predict" in verbs
    assert "extensions.market.batch_execute" in verbs
    assert "extensions.market.interpret_request" in verbs


def test_example_dataset_density_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
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
    assert result["average_degree"] == 2.0
    assert result["average_in_degree"] == 1.0
    assert result["average_out_degree"] == 1.0
    assert result["max_degree"] == 3
    assert result["min_degree"] == 1
    assert result["max_in_degree"] == 2
    assert result["min_in_degree"] == 0
    assert result["max_out_degree"] == 2
    assert result["min_out_degree"] == 0
    assert result["source_node_count"] == 2
    assert result["sink_node_count"] == 1
    assert result["isolated_node_count"] == 0


def test_example_connectivity_report_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
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
    assert body["status"] == "success"
    result = body["result"]
    assert result["connected"] is True
    assert result["path_count"] == 2
    assert result["truncated"] is False
    assert result["shortest_path_length"] == 2
    assert result["longest_path_length"] == 2
    assert result["all_paths"] == [
        ["product_quality", "demand", "revenue"],
        ["product_quality", "retention", "revenue"],
    ]


def test_example_path_contribution_report_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
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
    assert body["status"] == "success"
    result = body["result"]
    assert result["connected"] is True
    assert result["path_count"] == 2
    assert result["truncated"] is False
    assert result["total_effect"] == 3.0
    assert result["total_absolute_effect"] == 3.0
    assert result["top_contributing_path"] == ["product_quality", "demand", "revenue"]
    assert result["top_contributing_path_effect"] == 2.4
    assert [row["node_ids"] for row in result["paths"]] == [
        ["product_quality", "demand", "revenue"],
        ["product_quality", "retention", "revenue"],
    ]
    assert [row["path_effect"] for row in result["paths"]] == [2.4, 0.6]
    assert [row["share_of_total_effect"] for row in result["paths"]] == [0.8, 0.2]


def test_example_market_impact_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
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
    body = response.json()
    assert body["status"] == "success"
    result = body["result"]
    assert result["target_node"] == "marketing_spend"
    assert result["affected_node_count"] == 3
    assert result["downstream_affected_node_count"] == 2
    assert result["unaffected_node_count"] == 2
    assert result["market_change_level"] == 0.0175
    assert result["total_absolute_change"] == 2.8
    assert result["max_affected_node"] == "revenue"
    assert result["max_affected_change_abs"] == 1.2
    assert [row["node_id"] for row in result["affected_nodes"]] == [
        "revenue",
        "marketing_spend",
        "demand",
    ]


def test_example_node_systemic_risk_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
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
    body = response.json()
    assert body["status"] == "success"
    result = body["result"]
    assert result["node_id"] == "product_quality"
    assert result["node_known"] is True
    assert result["in_degree"] == 0
    assert result["out_degree"] == 2
    assert result["downstream_reachable_count"] == 3
    assert result["affected_node_count"] == 4
    assert result["downstream_affected_node_count"] == 3
    assert result["market_change_level"] == 0.0329
    assert result["concentration_risk"] == 0.7059
    assert result["structural_centrality"] == 0.25
    assert result["systemic_risk_score"] == 0.3707
    assert result["systemic_risk_level"] == "critical"
    assert [row["node_id"] for row in result["affected_nodes"]] == [
        "revenue",
        "demand",
        "product_quality",
        "retention",
    ]


def test_example_multi_intervention_impact_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
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
    body = response.json()
    assert body["status"] == "success"
    result = body["result"]
    assert result["intervention_count"] == 2
    assert result["evaluated_intervention_count"] == 2
    assert result["affected_node_count"] == 5
    assert result["unaffected_node_count"] == 0
    assert result["total_absolute_change"] == 8.05
    assert result["average_absolute_change"] == 1.61
    assert result["market_change_level"] == 0.0504
    assert result["max_affected_node"] == "revenue"
    assert result["max_affected_change_abs"] == 4.2
    assert [item["target_node"] for item in result["interventions"]] == [
        "marketing_spend",
        "product_quality",
    ]
    assert [row["node_id"] for row in result["affected_nodes"]] == [
        "revenue",
        "demand",
        "marketing_spend",
        "product_quality",
        "retention",
    ]
    assert [row["delta"] for row in result["affected_nodes"]] == [4.2, 1.8, 1.0, 1.0, 0.05]


def test_example_intervention_ranking_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
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
    body = response.json()
    assert body["status"] == "success"
    result = body["result"]
    assert result["outcome_node"] == "revenue"
    assert result["candidate_count"] == 4
    assert result["evaluated_candidate_count"] == 3
    assert result["ranked_candidate_count"] == 3
    assert result["missing_candidate_nodes"] == ["unknown_node"]
    assert [row["candidate_node"] for row in result["rankings"]] == [
        "product_quality",
        "demand",
        "marketing_spend",
    ]
    assert [row["effect_on_outcome"] for row in result["rankings"]] == [3.0, 2.0, 1.2]
    assert [row["rank"] for row in result["rankings"]] == [1, 2, 3]


def test_market_parse_request_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-market-parse",
            "verb": "extensions.market.parse_request",
            "params": {
                "request": {
                    "cap_version": "0.2.2",
                    "verb": "graph.neighbors",
                    "params": {"node_id": "demand", "scope": "parents"},
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["result"]["parsed_request"]["verb"] == "graph.neighbors"
    assert body["result"]["parsed_request"]["node_ids"] == ["demand"]
    assert "runtime_owner" in body["result"]["function_plan"]


def test_market_batch_execute_extension() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-market-batch",
            "verb": "extensions.market.batch_execute",
            "params": {
                "requests": [
                    {
                        "cap_version": "0.2.2",
                        "verb": "observe.predict",
                        "params": {"target_node": "revenue"},
                    },
                    {
                        "cap_version": "0.2.2",
                        "verb": "graph.neighbors",
                        "params": {"node_id": "demand", "scope": "parents", "max_neighbors": 5},
                    },
                ],
                "stop_on_error": False,
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["result"]["total_requests"] == 2
    assert body["result"]["executed_requests"] == 2
    assert body["result"]["failure_count"] == 0
    assert len(body["result"]["items"]) == 2


def test_cap_function_map_has_core_verbs() -> None:
    cap_map = load_cap_function_map()
    verb_map = cap_map["cap_verb_function_map"]
    assert "observe.predict" in verb_map
    assert "intervene.do" in verb_map
    assert "graph.neighbors" in verb_map
    assert "graph.markov_blanket" in verb_map
    assert "graph.paths" in verb_map
    assert "extensions.example.connectivity_report" in verb_map
    assert "extensions.example.path_contribution_report" in verb_map
    assert "extensions.example.market_impact" in verb_map
    assert "extensions.example.node_systemic_risk" in verb_map
    assert "extensions.example.multi_intervention_impact" in verb_map
    assert "extensions.example.intervention_ranking" in verb_map
    assert "extensions.example.dataset_density" in verb_map


def test_get_cap_function_plan_returns_mapping_for_verb() -> None:
    plan = get_cap_function_plan("observe.predict")
    assert plan["runtime_owner"] == "cap-example"
    assert "primary_functions" in plan


def test_market_interpret_request_supports_meta_methods_embedded_request() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-market-meta-methods",
            "verb": "extensions.market.interpret_request",
            "params": {
                "request": {
                    "cap_version": "0.2.2",
                    "verb": "meta.methods",
                    "params": {"detail": "compact"},
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    methods = body["result"]["calculation"]["methods"]
    assert any(item["verb"] == "observe.predict" for item in methods)
    assert any(item["verb"] == "extensions.market.batch_execute" for item in methods)
    assert any(item["verb"] == "extensions.market.interpret_request" for item in methods)


def test_market_interpret_request_supports_dataset_profile_embedded_request() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-market-dataset-profile",
            "verb": "extensions.market.interpret_request",
            "params": {
                "request": {
                    "cap_version": "0.2.2",
                    "verb": "extensions.example.dataset_profile",
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["result"]["calculation"]["graph_id"] == "toy-business-dag"


def test_market_interpret_request_supports_connectivity_report_embedded_request() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-market-connectivity-report",
            "verb": "extensions.market.interpret_request",
            "params": {
                "request": {
                    "cap_version": "0.2.2",
                    "verb": "extensions.example.connectivity_report",
                    "params": {
                        "source_node_id": "product_quality",
                        "target_node_id": "revenue",
                        "max_paths": 10,
                    },
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    calc = body["result"]["calculation"]
    assert calc["connected"] is True
    assert calc["path_count"] == 2
    assert calc["shortest_path_length"] == 2
    assert calc["longest_path_length"] == 2


def test_market_interpret_request_supports_path_contribution_report_embedded_request() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-market-path-contribution-report",
            "verb": "extensions.market.interpret_request",
            "params": {
                "request": {
                    "cap_version": "0.2.2",
                    "verb": "extensions.example.path_contribution_report",
                    "params": {
                        "source_node_id": "product_quality",
                        "target_node_id": "revenue",
                        "max_paths": 10,
                    },
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    calc = body["result"]["calculation"]
    assert calc["connected"] is True
    assert calc["path_count"] == 2
    assert calc["total_effect"] == 3.0
    assert calc["top_contributing_path_effect"] == 2.4
    assert [row["path_effect"] for row in calc["paths"]] == [2.4, 0.6]


def test_market_interpret_request_supports_market_impact_embedded_request() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-market-market-impact",
            "verb": "extensions.market.interpret_request",
            "params": {
                "request": {
                    "cap_version": "0.2.2",
                    "verb": "extensions.example.market_impact",
                    "params": {
                        "target_node": "marketing_spend",
                        "intervention_delta": 1.0,
                        "min_effect_threshold": 0.0001,
                    },
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    calc = body["result"]["calculation"]
    assert calc["affected_node_count"] == 3
    assert calc["downstream_affected_node_count"] == 2
    assert calc["market_change_level"] == 0.0175
    assert calc["max_affected_node"] == "revenue"
    assert calc["max_affected_change_abs"] == 1.2


def test_market_interpret_request_supports_node_systemic_risk_embedded_request() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-market-node-systemic-risk",
            "verb": "extensions.market.interpret_request",
            "params": {
                "request": {
                    "cap_version": "0.2.2",
                    "verb": "extensions.example.node_systemic_risk",
                    "params": {
                        "node_id": "product_quality",
                        "stress_delta": 1.0,
                        "min_effect_threshold": 0.0001,
                    },
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    calc = body["result"]["calculation"]
    assert calc["node_known"] is True
    assert calc["affected_node_count"] == 4
    assert calc["downstream_affected_node_count"] == 3
    assert calc["market_change_level"] == 0.0329
    assert calc["systemic_risk_score"] == 0.3707
    assert calc["systemic_risk_level"] == "critical"
    assert [row["node_id"] for row in calc["affected_nodes"]] == [
        "revenue",
        "demand",
        "product_quality",
        "retention",
    ]


def test_market_interpret_request_supports_multi_intervention_impact_embedded_request() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-market-multi-intervention-impact",
            "verb": "extensions.market.interpret_request",
            "params": {
                "request": {
                    "cap_version": "0.2.2",
                    "verb": "extensions.example.multi_intervention_impact",
                    "params": {
                        "interventions": [
                            {"target_node": "marketing_spend", "intervention_delta": 1.0},
                            {"target_node": "product_quality", "intervention_delta": 1.0},
                        ],
                        "min_effect_threshold": 0.0001,
                    },
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    calc = body["result"]["calculation"]
    assert calc["intervention_count"] == 2
    assert calc["evaluated_intervention_count"] == 2
    assert calc["affected_node_count"] == 5
    assert calc["market_change_level"] == 0.0504
    assert calc["max_affected_node"] == "revenue"
    assert calc["max_affected_change_abs"] == 4.2
    assert [row["delta"] for row in calc["affected_nodes"]] == [4.2, 1.8, 1.0, 1.0, 0.05]


def test_market_interpret_request_supports_intervention_ranking_embedded_request() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-market-intervention-ranking",
            "verb": "extensions.market.interpret_request",
            "params": {
                "request": {
                    "cap_version": "0.2.2",
                    "verb": "extensions.example.intervention_ranking",
                    "params": {
                        "outcome_node": "revenue",
                        "intervention_delta": 1.0,
                        "candidate_nodes": [
                            "marketing_spend",
                            "product_quality",
                            "demand",
                        ],
                        "top_k": 3,
                        "min_effect_threshold": 0.0001,
                    },
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    calc = body["result"]["calculation"]
    assert calc["ranked_candidate_count"] == 3
    assert [row["candidate_node"] for row in calc["rankings"]] == [
        "product_quality",
        "demand",
        "marketing_spend",
    ]
    assert [row["effect_on_outcome"] for row in calc["rankings"]] == [3.0, 2.0, 1.2]


def test_market_interpret_request_supports_dataset_density_embedded_request() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-market-dataset-density",
            "verb": "extensions.market.interpret_request",
            "params": {
                "request": {
                    "cap_version": "0.2.2",
                    "verb": "extensions.example.dataset_density",
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    calc = body["result"]["calculation"]
    assert calc["possible_directed_edges"] == 20
    assert calc["missing_directed_edges"] == 15
    assert calc["density"] == 0.25
    assert calc["sparsity"] == 0.75
    assert calc["max_degree"] == 3
    assert calc["min_degree"] == 1


def test_market_interpret_request_supports_verb_catalog_embedded_request() -> None:
    response = client.post(
        "/cap",
        json={
            "cap_version": "0.2.2",
            "request_id": "req-market-verb-catalog",
            "verb": "extensions.market.interpret_request",
            "params": {
                "request": {
                    "cap_version": "0.2.2",
                    "verb": "extensions.example.verb_catalog",
                    "params": {"detail": "compact", "include_examples": False},
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    methods = body["result"]["calculation"]["methods"]
    assert any(item["verb"] == "extensions.example.verb_catalog" for item in methods)
