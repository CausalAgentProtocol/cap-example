from __future__ import annotations

from pathlib import Path

from example_cap_server import runtime_config


def test_merge_market_options_without_config(monkeypatch) -> None:
    missing_path = Path("/tmp/cap-example-this-file-does-not-exist.toml")
    monkeypatch.setenv(runtime_config.CONFIG_PATH_ENV, str(missing_path))
    runtime_config.reset_runtime_config_cache()

    merged = runtime_config.merge_market_options(
        {"graph_operations": {"local_graph": {"max_hops": 2}}}
    )
    assert merged["graph_operations"]["local_graph"]["max_hops"] == 2

    metadata = runtime_config.runtime_config_metadata()
    assert metadata["loaded"] is False


def test_merge_market_options_from_toml(monkeypatch, tmp_path: Path) -> None:
    config_file = tmp_path / "cap-example.toml"
    config_file.write_text(
        """
[market_interpretation.default_options.graph_operations.local_graph]
max_hops = 1
max_nodes = 64
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setenv(runtime_config.CONFIG_PATH_ENV, str(config_file))
    runtime_config.reset_runtime_config_cache()

    merged = runtime_config.merge_market_options(
        {"graph_operations": {"local_graph": {"max_hops": 3}}}
    )
    assert merged["graph_operations"]["local_graph"]["max_hops"] == 3
    assert merged["graph_operations"]["local_graph"]["max_nodes"] == 64

    metadata = runtime_config.runtime_config_metadata()
    assert metadata["loaded"] is True
    assert metadata["source"] == "env"
    assert metadata["path"] == str(config_file.resolve())

    debug_view = runtime_config.runtime_config_debug_view()
    assert (
        debug_view["market_default_options"]["graph_operations"]["local_graph"]["max_nodes"]
        == 64
    )
