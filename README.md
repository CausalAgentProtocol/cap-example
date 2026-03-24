# cap-example

Official minimal example server for the Causal Agent Protocol (CAP).

This example exists to teach the CAP protocol boundary without dragging in product-specific topology, proprietary discovery semantics, or vendor-backed runtime assumptions. It is intentionally small, synthetic, and easy to replace.

If you are new to CAP, start with the [`cap` repository](https://github.com/CausalAgentProtocol/cap) for the protocol overview, getting-started guides, and normative specification. Use this repository when you want to inspect the smallest honest CAP server that still feels like a real service.

## Choose The Right Repo

- [`cap`](https://github.com/CausalAgentProtocol/cap): learn CAP and read the authoritative protocol docs
- [`python-sdk`](https://github.com/CausalAgentProtocol/python-sdk): build CAP clients and CAP-compatible Python services
- `cap-example`: inspect a neutral, synthetic CAP server that demonstrates the protocol boundary

## What This Repository Demonstrates

This example server demonstrates:

- `GET /.well-known/cap.json` for machine-readable capability discovery
- one `POST /cap` entrypoint for all CAP verbs
- honest Level 2 disclosure over a synthetic in-memory graph
- the difference between CAP core verbs, convenience verbs, and one namespaced extension
- protocol-first handler structure using the official Python SDK

This repository does not demonstrate:

- a production deployment pattern
- proprietary graph discovery or refresh pipelines
- domain-specific extension strategy
- real customer data or a scientifically authoritative causal model

## Why This Example Exists

The CAP organization should own the protocol contract, not a single vendor's product adapter.

That means the official example should be:

- neutral
- synthetic
- small enough to understand in one sitting
- honest about what is toy behavior and what is protocol behavior

This repository is therefore a teaching artifact, not a canonical scientific implementation.

## Repository Layout

- `example_cap_server/main.py`: FastAPI app, CAP registry, capability card, and handlers
- `example_cap_server/toy_graph.py`: synthetic graph data and deterministic toy runtime
- `tests/test_app.py`: smoke tests for capability discovery and core CAP routes

## Run It Locally

Prerequisites:

- Python 3.11+
- `uv`

Install and run locally:

```bash
uv sync --extra dev
uv run uvicorn example_cap_server.main:app --reload --host 0.0.0.0 --port 8000
```

Then inspect:

- `http://127.0.0.1:8000/.well-known/cap.json`
- `http://127.0.0.1:8000/docs`
- `http://127.0.0.1:8000/health`

## Try The CAP Surface

Fetch capabilities:

```bash
curl -s -X POST http://127.0.0.1:8000/cap \
  -H 'Content-Type: application/json' \
  -d '{
    "cap_version": "0.2.2",
    "request_id": "req-capabilities-1",
    "verb": "meta.capabilities"
  }' | jq
```

Inspect method descriptors:

```bash
curl -s -X POST http://127.0.0.1:8000/cap \
  -H 'Content-Type: application/json' \
  -d '{
    "cap_version": "0.2.2",
    "request_id": "req-methods-1",
    "verb": "meta.methods",
    "params": {
      "detail": "full"
    }
  }' | jq
```

Query graph neighbors:

```bash
curl -s -X POST http://127.0.0.1:8000/cap \
  -H 'Content-Type: application/json' \
  -d '{
    "cap_version": "0.2.2",
    "request_id": "req-neighbors-1",
    "verb": "graph.neighbors",
    "params": {
      "node_id": "demand",
      "scope": "parents",
      "max_neighbors": 5
    }
  }' | jq
```

Run a toy intervention:

```bash
curl -s -X POST http://127.0.0.1:8000/cap \
  -H 'Content-Type: application/json' \
  -d '{
    "cap_version": "0.2.2",
    "request_id": "req-intervene-1",
    "verb": "intervene.do",
    "params": {
      "treatment_node": "marketing_spend",
      "treatment_value": 2.0,
      "outcome_node": "revenue"
    }
  }' | jq
```

Inspect the namespaced example extension:

```bash
curl -s -X POST http://127.0.0.1:8000/cap \
  -H 'Content-Type: application/json' \
  -d '{
    "cap_version": "0.2.2",
    "request_id": "req-profile-1",
    "verb": "extensions.example.dataset_profile"
  }' | jq
```

## Reading The Responses Correctly

This server uses a synthetic directed acyclic graph and simple deterministic coefficients. It is useful for teaching CAP semantics, but it should not be mistaken for a real scientific system.

The important lesson is not the numeric values. The important lesson is:

- how capabilities are disclosed before invocation
- how one CAP envelope surface dispatches multiple verbs
- how semantic honesty fields travel with stronger claims
- how extensions stay namespaced instead of pretending to be CAP core

## License

Apache-2.0.

## Community

Repository-level license and CI configuration live in this repository.
Contribution guidelines, security policy, code of conduct, support guidance,
and issue or pull request templates are managed at the
`CausalAgentProtocol/.github` organization level.
