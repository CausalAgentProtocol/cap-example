from __future__ import annotations

from typing import Any

from example_cap_server import toy_graph
from example_cap_server.market_pipeline.utils import dedupe_preserve, is_known_node

EQUITY_CLASSIFICATION = {
    "AAPL": ("equity", "technology", "consumer_hardware"),
    "MSFT": ("equity", "technology", "software_platform"),
    "NVDA": ("equity", "technology", "semiconductors"),
    "TSLA": ("equity", "consumer_discretionary", "ev_auto"),
    "COIN": ("equity", "financials", "crypto_exchange"),
}

CRYPTO_CLASSIFICATION = {
    "BTC": ("crypto", "digital_assets", "store_of_value"),
    "ETH": ("crypto", "digital_assets", "smart_contract_l1"),
    "SOL": ("crypto", "digital_assets", "high_throughput_l1"),
    "BNB": ("crypto", "digital_assets", "exchange_ecosystem"),
    "XRP": ("crypto", "digital_assets", "payments_network"),
}

METRIC_TEMPLATES = {
    "price": (
        "Price nodes often reflect valuation, aggregate expectations, and "
        "risk appetite."
    ),
    "volume": (
        "Volume nodes often reflect participation, liquidity, and information "
        "arrival intensity."
    ),
    "return": (
        "Return nodes often reflect directional performance and shock "
        "transmission."
    ),
    "volatility": (
        "Volatility nodes often capture uncertainty and regime stress."
    ),
    "market_cap": (
        "Market cap nodes often reflect aggregate valuation and size effects."
    ),
    "unknown": (
        "Node semantics are domain-defined; classify using your own graph "
        "dictionary."
    ),
}


def build_postprocess(
    *,
    focal_node_ids: list[str],
    graph_excerpt: dict[str, Any],
) -> dict[str, Any]:
    parents_map, children_map = _adjacency_from_graph_excerpt(graph_excerpt)
    interpreted_nodes = dedupe_preserve(
        focal_node_ids + [node["node_id"] for node in graph_excerpt["available_nodes"]]
    )
    node_properties = [describe_node(node_id) for node_id in interpreted_nodes]
    parent_neighbor_summary = [
        summarize_parents_and_neighbors(
            node_id,
            parents_map=parents_map,
            children_map=children_map,
        )
        for node_id in focal_node_ids
    ]
    return {
        "node_properties": node_properties,
        "parent_neighbor_summary": parent_neighbor_summary,
    }


def describe_node(node_id: str) -> dict[str, Any]:
    symbol, metric_type = extract_symbol_and_metric(node_id)
    asset_class = "unknown"
    sector = "unknown"
    subsector = "unknown"

    if symbol in EQUITY_CLASSIFICATION:
        asset_class, sector, subsector = EQUITY_CLASSIFICATION[symbol]
    elif symbol in CRYPTO_CLASSIFICATION:
        asset_class, sector, subsector = CRYPTO_CLASSIFICATION[symbol]
    elif is_known_node(node_id):
        asset_class = "graph_internal"
        sector = toy_graph.NODE_DOMAINS.get(node_id, "unknown")
        subsector = toy_graph.NODE_TYPES.get(node_id, "unknown")

    return {
        "node_id": node_id,
        "asset_symbol": symbol,
        "asset_class": asset_class,
        "metric_type": metric_type,
        "sector": sector,
        "subsector": subsector,
        "description": METRIC_TEMPLATES.get(metric_type, METRIC_TEMPLATES["unknown"]),
    }


def summarize_parents_and_neighbors(
    node_id: str,
    *,
    parents_map: dict[str, list[str]],
    children_map: dict[str, list[str]],
) -> dict[str, Any]:
    parents = parents_map.get(node_id, [])
    children = children_map.get(node_id, [])
    if not parents and not children and is_known_node(node_id):
        parents = toy_graph.neighbors(node_id, "parents")
        children = toy_graph.neighbors(node_id, "children")

    if not parents and not children and not is_known_node(node_id):
        return {
            "node_id": node_id,
            "parents": [],
            "children": [],
            "neighbors": [],
            "summary": (
                "This node is not present in the currently mounted graph, so "
                "parent and neighbor meaning cannot be inferred from structure."
            ),
        }

    neighbors = sorted(set(parents + children))

    node_profile = describe_node(node_id)
    parent_profiles = [describe_node(parent_id) for parent_id in parents]

    if not parents:
        upstream_text = (
            "No parents are currently modeled, so this node is treated as "
            "exogenous in the active graph."
        )
    else:
        upstream_roles = describe_upstream_roles(node_profile, parent_profiles)
        upstream_text = (
            f"Parents represent immediate upstream causes. {upstream_roles} "
            f"Current parents: {', '.join(parents)}."
        )

    if children:
        downstream_text = (
            "Children represent immediate downstream propagation targets. "
            f"Current children: {', '.join(children)}."
        )
    else:
        downstream_text = (
            "No children are modeled, so downstream propagation is not "
            "represented."
        )

    return {
        "node_id": node_id,
        "parents": parents,
        "children": children,
        "neighbors": neighbors,
        "summary": f"{upstream_text} {downstream_text}",
    }


def _adjacency_from_graph_excerpt(
    graph_excerpt: dict[str, Any],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    parents_map: dict[str, set[str]] = {}
    children_map: dict[str, set[str]] = {}
    for edge in graph_excerpt.get("edges", []):
        source = edge.get("source")
        target = edge.get("target")
        if not isinstance(source, str) or not source:
            continue
        if not isinstance(target, str) or not target:
            continue
        parents_map.setdefault(target, set()).add(source)
        children_map.setdefault(source, set()).add(target)
    parents = {node_id: sorted(values) for node_id, values in parents_map.items()}
    children = {node_id: sorted(values) for node_id, values in children_map.items()}
    return parents, children


def describe_upstream_roles(
    node_profile: dict[str, Any],
    parent_profiles: list[dict[str, Any]],
) -> str:
    node_metric = node_profile["metric_type"]
    parent_metrics = {profile["metric_type"] for profile in parent_profiles}

    if node_metric == "price":
        if "volume" in parent_metrics:
            return (
                "Volume-like parents may represent liquidity and participation "
                "pressure on price formation."
            )
        if "price" in parent_metrics:
            return (
                "Price-like parents may represent cross-asset spillover or "
                "benchmark leadership effects."
            )
    if node_metric == "volume":
        if "price" in parent_metrics or "volatility" in parent_metrics:
            return (
                "Price/volatility-like parents may represent risk and attention "
                "regimes that drive trading activity."
            )

    return (
        "These parents should be interpreted as first-order structural drivers "
        "for the focal node under the current graph assumptions."
    )


def extract_symbol_and_metric(node_id: str) -> tuple[str | None, str]:
    normalized = node_id.lower()
    if "." in node_id:
        parts = [part for part in normalized.split(".") if part]
        if len(parts) >= 2:
            symbol = parts[-2].replace("-", "_").upper()
            metric_alias = {
                "prices": "price",
                "price": "price",
                "total_volumes": "volume",
                "volume": "volume",
                "market_caps": "market_cap",
                "market_cap": "market_cap",
                "returns": "return",
                "return": "return",
                "volatility": "volatility",
            }
            metric = metric_alias.get(parts[-1], "unknown")
            return symbol, metric

    suffixes = {
        "_price": "price",
        "_volume": "volume",
        "_return": "return",
        "_returns": "return",
        "_volatility": "volatility",
    }
    prefixes = {
        "price_": "price",
        "volume_": "volume",
        "return_": "return",
        "volatility_": "volatility",
    }

    for suffix, metric in suffixes.items():
        if normalized.endswith(suffix):
            symbol = node_id[: -len(suffix)].strip("_").upper() or None
            return symbol, metric

    for prefix, metric in prefixes.items():
        if normalized.startswith(prefix):
            symbol = node_id[len(prefix) :].strip("_").upper() or None
            return symbol, metric

    cleaned = node_id.replace("_", "").upper()
    if cleaned.isalpha() and 2 <= len(cleaned) <= 5:
        return cleaned, "price"

    return None, "unknown"
