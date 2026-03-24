from fastapi.testclient import TestClient

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
