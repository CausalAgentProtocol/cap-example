# cap-example

Official minimal example server for the Causal Agent Protocol (CAP).

This repository is a toy-graph-only teaching server:

- no Neo4j
- no external graph runtime
- no LLM requirement

It focuses on CAP verb usage and extension design.

## What This Repo Shows

- `GET /.well-known/cap.json` capability discovery
- one `POST /cap` endpoint for all verbs
- CAP core + convenience verbs
- multiple namespaced extension verbs

## Run Locally

Prerequisites:

- Python 3.11+

Install and run:

```bash
python -m pip install -e .
python -m pip install pytest
uvicorn example_cap_server.main:app --reload --host 0.0.0.0 --port 8000
```

Inspect:

- `http://127.0.0.1:8000/.well-known/cap.json`
- `http://127.0.0.1:8000/docs`
- `http://127.0.0.1:8000/health`

## Full Verb Suite

Run all direct examples:

```bash
python scripts/run_cap_verb_suite.py --base-url http://127.0.0.1:8000
```

Reports are written to `reports/`.

## CAP Verb Examples

### 1) Core / convenience examples

```bash
curl -s -X POST http://127.0.0.1:8000/cap -H 'Content-Type: application/json' -d '{
  "cap_version": "0.2.2",
  "request_id": "req-meta-capabilities",
  "verb": "meta.capabilities"
}' | jq
```

```bash
curl -s -X POST http://127.0.0.1:8000/cap -H 'Content-Type: application/json' -d '{
  "cap_version": "0.2.2",
  "request_id": "req-observe",
  "verb": "observe.predict",
  "params": {"target_node": "revenue"}
}' | jq
```

```bash
curl -s -X POST http://127.0.0.1:8000/cap -H 'Content-Type: application/json' -d '{
  "cap_version": "0.2.2",
  "request_id": "req-neighbors",
  "verb": "graph.neighbors",
  "params": {"node_id": "demand", "scope": "parents", "max_neighbors": 5}
}' | jq
```

### 2) Extension examples

```bash
curl -s -X POST http://127.0.0.1:8000/cap -H 'Content-Type: application/json' -d '{
  "cap_version": "0.2.2",
  "request_id": "req-dataset",
  "verb": "extensions.example.dataset_profile"
}' | jq
```

```bash
curl -s -X POST http://127.0.0.1:8000/cap -H 'Content-Type: application/json' -d '{
  "cap_version": "0.2.2",
  "request_id": "req-dataset-density",
  "verb": "extensions.example.dataset_density"
}' | jq
```

Returns density-oriented graph stats including `possible_directed_edges`,
`missing_directed_edges`, `sparsity`, in/out degree extrema, and source/sink counts.

```bash
curl -s -X POST http://127.0.0.1:8000/cap -H 'Content-Type: application/json' -d '{
  "cap_version": "0.2.2",
  "request_id": "req-connectivity",
  "verb": "extensions.example.connectivity_report",
  "params": {
    "source_node_id": "product_quality",
    "target_node_id": "revenue",
    "max_paths": 10
  }
}' | jq
```

```bash
curl -s -X POST http://127.0.0.1:8000/cap -H 'Content-Type: application/json' -d '{
  "cap_version": "0.2.2",
  "request_id": "req-path-contrib",
  "verb": "extensions.example.path_contribution_report",
  "params": {
    "source_node_id": "product_quality",
    "target_node_id": "revenue",
    "max_paths": 10
  }
}' | jq
```

```bash
curl -s -X POST http://127.0.0.1:8000/cap -H 'Content-Type: application/json' -d '{
  "cap_version": "0.2.2",
  "request_id": "req-market-impact",
  "verb": "extensions.example.market_impact",
  "params": {
    "target_node": "marketing_spend",
    "intervention_delta": 1.0,
    "min_effect_threshold": 0.0001
  }
}' | jq
```

```bash
curl -s -X POST http://127.0.0.1:8000/cap -H 'Content-Type: application/json' -d '{
  "cap_version": "0.2.2",
  "request_id": "req-node-risk",
  "verb": "extensions.example.node_systemic_risk",
  "params": {
    "node_id": "product_quality",
    "stress_delta": 1.0,
    "min_effect_threshold": 0.0001
  }
}' | jq
```

```bash
curl -s -X POST http://127.0.0.1:8000/cap -H 'Content-Type: application/json' -d '{
  "cap_version": "0.2.2",
  "request_id": "req-multi-impact",
  "verb": "extensions.example.multi_intervention_impact",
  "params": {
    "interventions": [
      {"target_node": "marketing_spend", "intervention_delta": 1.0},
      {"target_node": "product_quality", "intervention_delta": 1.0}
    ],
    "min_effect_threshold": 0.0001
  }
}' | jq
```

```bash
curl -s -X POST http://127.0.0.1:8000/cap -H 'Content-Type: application/json' -d '{
  "cap_version": "0.2.2",
  "request_id": "req-intervention-ranking",
  "verb": "extensions.example.intervention_ranking",
  "params": {
    "outcome_node": "revenue",
    "intervention_delta": 1.0,
    "candidate_nodes": ["marketing_spend", "product_quality", "demand"],
    "top_k": 3,
    "min_effect_threshold": 0.0001
  }
}' | jq
```

```bash
curl -s -X POST http://127.0.0.1:8000/cap -H 'Content-Type: application/json' -d '{
  "cap_version": "0.2.2",
  "request_id": "req-catalog",
  "verb": "extensions.example.verb_catalog",
  "params": {"detail": "full", "include_examples": true}
}' | jq
```

## Verb Coverage Table

| Verb | Type | Required params |
| --- | --- | --- |
| `meta.capabilities` | core | none |
| `meta.methods` | core | optional `detail` |
| `observe.predict` | core | `target_node` |
| `intervene.do` | core | `treatment_node`, `treatment_value`, `outcome_node` |
| `graph.neighbors` | core | `node_id`, `scope` |
| `graph.markov_blanket` | core | `node_id` |
| `graph.paths` | core | `source_node_id`, `target_node_id` |
| `traverse.parents` | convenience | `node_id`, optional `top_k` |
| `traverse.children` | convenience | `node_id`, optional `top_k` |
| `extensions.example.dataset_profile` | extension | none |
| `extensions.example.connectivity_report` | extension | `source_node_id`, `target_node_id`, optional `max_paths` |
| `extensions.example.path_contribution_report` | extension | `source_node_id`, `target_node_id`, optional `max_paths` |
| `extensions.example.market_impact` | extension | `target_node`, optional `intervention_delta`, optional `min_effect_threshold` |
| `extensions.example.node_systemic_risk` | extension | `node_id`, optional `stress_delta`, optional `min_effect_threshold` |
| `extensions.example.multi_intervention_impact` | extension | `interventions[]` with `target_node`, optional `intervention_delta`, optional `min_effect_threshold` |
| `extensions.example.intervention_ranking` | extension | `outcome_node`, optional `intervention_delta`, optional `candidate_nodes`, optional `top_k`, optional `min_effect_threshold` |
| `extensions.example.node_criticality_ranking` | extension | optional `candidate_nodes`, optional `stress_delta`, optional `top_k`, optional `min_effect_threshold` |
| `extensions.example.edge_criticality_ranking` | extension | optional `top_k` |
| `extensions.example.goal_seek_intervention` | extension | `outcome_node`, `target_outcome_change`, optional `candidate_nodes`, optional `max_plans`, optional `min_effect_threshold` |
| `extensions.example.budgeted_intervention_optimizer` | extension | `outcome_node`, `budget`, optional `objective`, optional `candidate_nodes`, optional `max_allocations`, optional `min_effect_threshold` |
| `extensions.example.pareto_intervention_frontier` | extension | `outcome_node`, optional `intervention_delta`, optional `objective`, optional `candidate_nodes`, optional `min_effect_threshold` |
| `extensions.example.scenario_compare` | extension | `scenarios[]` with `interventions[]`, optional `outcome_node`, optional `min_effect_threshold` |
| `extensions.example.shock_cascade_simulation` | extension | `target_node`, optional `shock_delta`, optional `steps`, optional `damping`, optional `min_effect_threshold` |
| `extensions.example.resilience_report` | extension | optional `top_k`, optional `min_effect_threshold` |
| `extensions.example.target_vulnerability_report` | extension | `target_node`, optional `shock_delta`, optional `candidate_sources`, optional `top_k`, optional `min_effect_threshold` |
| `extensions.example.bottleneck_report` | extension | optional `top_k`, optional `max_paths_per_pair` |
| `extensions.example.influence_matrix` | extension | optional `node_ids` |
| `extensions.example.intervention_battle` | extension | `outcome_node`, `plan_a`, `plan_b`, optional `min_effect_threshold`, optional `disruption_penalty` |
| `extensions.example.dataset_density` | extension | none |
| `extensions.example.verb_catalog` | extension | optional `detail`, `include_examples` |

## Testing

```bash
python -m pytest -q
```

## License

Apache-2.0.
