from __future__ import annotations

from typing import Any

from example_cap_server.market_pipeline import run_market_interpretation_pipeline


async def interpret_cap_request(
    request_payload: dict[str, Any],
    *,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return await run_market_interpretation_pipeline(request_payload, options=options)
