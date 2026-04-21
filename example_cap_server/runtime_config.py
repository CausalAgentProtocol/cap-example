from __future__ import annotations

import copy
import os
import tomllib
from pathlib import Path
from typing import Any


CONFIG_PATH_ENV = "CAP_EXAMPLE_CONFIG_FILE"
DEFAULT_CONFIG_RELATIVE = Path("config") / "cap-example.toml"

_CONFIG_CACHE: dict[str, Any] | None = None
_CONFIG_META_CACHE: dict[str, Any] | None = None


def reset_runtime_config_cache() -> None:
    global _CONFIG_CACHE, _CONFIG_META_CACHE
    _CONFIG_CACHE = None
    _CONFIG_META_CACHE = None


def merge_market_options(request_options: dict[str, Any] | None) -> dict[str, Any]:
    merged = get_market_default_options()
    if isinstance(request_options, dict):
        _deep_update(merged, request_options)
    return merged


def get_market_default_options() -> dict[str, Any]:
    config = _get_runtime_config()
    market = config.get("market_interpretation", {})
    if not isinstance(market, dict):
        return {}
    default_options = market.get("default_options", {})
    if not isinstance(default_options, dict):
        return {}
    return copy.deepcopy(default_options)


def runtime_config_metadata() -> dict[str, Any]:
    _ensure_loaded()
    return copy.deepcopy(_CONFIG_META_CACHE or {})


def runtime_config_debug_view() -> dict[str, Any]:
    config = _get_runtime_config()
    return {
        "metadata": runtime_config_metadata(),
        "market_default_options": _mask_sensitive(get_market_default_options()),
        "raw_top_level_keys": sorted(config.keys()) if isinstance(config, dict) else [],
    }


def _get_runtime_config() -> dict[str, Any]:
    _ensure_loaded()
    return copy.deepcopy(_CONFIG_CACHE or {})


def _ensure_loaded() -> None:
    global _CONFIG_CACHE, _CONFIG_META_CACHE
    if _CONFIG_CACHE is not None and _CONFIG_META_CACHE is not None:
        return

    path, source = _resolve_config_path()
    if path is None:
        _CONFIG_CACHE = {}
        _CONFIG_META_CACHE = {
            "loaded": False,
            "source": source,
            "path": None,
            "error": None,
        }
        return

    try:
        with path.open("rb") as handle:
            parsed = tomllib.load(handle)
        if not isinstance(parsed, dict):
            raise ValueError("Runtime config must be a TOML object.")
        _CONFIG_CACHE = parsed
        _CONFIG_META_CACHE = {
            "loaded": True,
            "source": source,
            "path": str(path),
            "error": None,
        }
    except Exception as error:
        _CONFIG_CACHE = {}
        _CONFIG_META_CACHE = {
            "loaded": False,
            "source": source,
            "path": str(path),
            "error": str(error),
        }


def _resolve_config_path() -> tuple[Path | None, str]:
    explicit = os.getenv(CONFIG_PATH_ENV)
    if explicit:
        return Path(explicit).expanduser().resolve(), "env"

    cwd_candidate = Path.cwd() / DEFAULT_CONFIG_RELATIVE
    if cwd_candidate.is_file():
        return cwd_candidate.resolve(), "default"

    repo_candidate = Path(__file__).resolve().parents[1] / DEFAULT_CONFIG_RELATIVE
    if repo_candidate.is_file():
        return repo_candidate.resolve(), "default"

    return None, "default"


def _deep_update(base: dict[str, Any], override: dict[str, Any]) -> None:
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = copy.deepcopy(value)


def _mask_sensitive(value: Any, *, key_hint: str | None = None) -> Any:
    if isinstance(value, dict):
        return {
            key: _mask_sensitive(item, key_hint=key)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_mask_sensitive(item, key_hint=key_hint) for item in value]
    if isinstance(value, str) and _is_sensitive_key(key_hint):
        return "***"
    return value


def _is_sensitive_key(key: str | None) -> bool:
    if not key:
        return False
    lowered = key.lower()
    return any(token in lowered for token in ("password", "secret", "token"))
