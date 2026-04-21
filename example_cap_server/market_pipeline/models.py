from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ParsedCAPRequest:
    verb: str
    params: dict[str, Any]
    node_ids: list[str]
