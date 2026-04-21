from __future__ import annotations

import json
from pathlib import Path
from typing import Any


MAP_PATH = Path(__file__).with_name("abel_graph_computer_cap_map.json")


def load_cap_function_map() -> dict[str, Any]:
    with MAP_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def get_cap_function_plan(verb: str) -> dict[str, Any]:
    mapping = load_cap_function_map()
    return mapping.get("cap_verb_function_map", {}).get(verb, {})
