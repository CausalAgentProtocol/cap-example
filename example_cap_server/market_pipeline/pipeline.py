from __future__ import annotations

from typing import Any

from example_cap_server.market_pipeline.analysis import build_analysis
from example_cap_server.market_pipeline.graph_ops import build_local_graph, calculate_verb_result
from example_cap_server.market_pipeline.parser import parse_cap_request
from example_cap_server.market_pipeline.postprocess import build_postprocess


async def run_market_interpretation_pipeline(
    request_payload: dict[str, Any],
    *,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        parsed = parse_cap_request(request_payload)
    except ValueError as error:
        return _build_parse_error_result(str(error))

    parsed_request = {
        "verb": parsed.verb,
        "node_ids": parsed.node_ids,
        "params": parsed.params,
    }
    graph_excerpt = await build_local_graph(parsed, options=options)
    calculation = await calculate_verb_result(parsed, options=options)
    postprocess = build_postprocess(
        focal_node_ids=parsed.node_ids,
        graph_excerpt=graph_excerpt,
    )
    analysis = build_analysis(
        parsed=parsed,
        graph_excerpt=graph_excerpt,
        calculation=calculation,
        postprocess=postprocess,
    )

    return {
        "stages": {
            "parse": {
                "status": "success",
                "data": parsed_request,
            },
            "graph_operations": {
                "status": "success",
                "data": graph_excerpt,
            },
            "calculation": {
                "status": calculation.get("status", "success"),
                "data": calculation,
            },
            "postprocess": {
                "status": "success",
                "data": postprocess,
            },
            "analysis": {
                "status": "success",
                "data": analysis,
            },
        },
        "parsed_request": parsed_request,
        "graph_excerpt": graph_excerpt,
        "calculation": calculation,
        "node_properties": postprocess["node_properties"],
        "parent_neighbor_summary": postprocess["parent_neighbor_summary"],
        "analysis": analysis,
    }


def _build_parse_error_result(message: str) -> dict[str, Any]:
    return {
        "stages": {
            "parse": {
                "status": "error",
                "message": message,
            },
            "graph_operations": {
                "status": "skipped",
                "message": "Skipped because parse stage failed.",
                "data": {},
            },
            "calculation": {
                "status": "skipped",
                "message": "Skipped because parse stage failed.",
                "data": {},
            },
            "postprocess": {
                "status": "skipped",
                "message": "Skipped because parse stage failed.",
                "data": {},
            },
            "analysis": {
                "status": "skipped",
                "message": "Skipped because parse stage failed.",
                "data": {},
            },
        },
        "parsed_request": {
            "verb": None,
            "node_ids": [],
            "params": {},
        },
        "graph_excerpt": {
            "requested_nodes": [],
            "available_nodes": [],
            "missing_nodes": [],
            "edges": [],
        },
        "calculation": {
            "status": "invalid_request",
            "message": message,
        },
        "analysis": {},
        "node_properties": [],
        "parent_neighbor_summary": [],
    }
