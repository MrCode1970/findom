from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class MechanismConfig:
    browser: dict[str, Any]
    capture: dict[str, Any]
    redaction: dict[str, Any]
    artifacts: dict[str, Any]
    logging: dict[str, Any]


@dataclass
class TargetConfig:
    provider_name: str
    start_url: str
    start_urls: list[str]
    env: dict[str, Any]
    ui: dict[str, Any]
    reports: dict[str, Any]
    endpoint_hints: dict[str, Any]
    artifacts: dict[str, Any]
    compatibility: dict[str, Any]


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    text = path.read_text(encoding="utf-8")
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Config {path} must be JSON-compatible YAML (JSON syntax in .yaml file): {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a mapping: {path}")
    return data


def load_configs(mechanism_path: Path, target_path: Path) -> tuple[MechanismConfig, TargetConfig]:
    mechanism_raw = _load_yaml(mechanism_path)
    target_raw = _load_yaml(target_path)

    mechanism = MechanismConfig(
        browser=mechanism_raw.get("browser", {}),
        capture=mechanism_raw.get("capture", {}),
        redaction=mechanism_raw.get("redaction", {}),
        artifacts=mechanism_raw.get("artifacts", {}),
        logging=mechanism_raw.get("logging", {}),
    )

    required = ["provider_name", "start_url"]
    missing = [key for key in required if not target_raw.get(key)]
    if missing:
        raise ValueError(f"Target config missing required fields: {', '.join(missing)}")

    target = TargetConfig(
        provider_name=target_raw["provider_name"],
        start_url=target_raw["start_url"],
        start_urls=target_raw.get("start_urls", []),
        env=target_raw.get("env", {}),
        ui=target_raw.get("ui", {}),
        reports=target_raw.get("reports", {}),
        endpoint_hints=target_raw.get("endpoint_hints", {}),
        artifacts=target_raw.get("artifacts", {}),
        compatibility=target_raw.get("compatibility", {}),
    )
    return mechanism, target
