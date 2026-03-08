from __future__ import annotations

from typing import Any


def redact_headers(headers: dict[str, str], sensitive_markers: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    lowered = [item.lower() for item in sensitive_markers]
    for key, value in headers.items():
        key_l = key.lower()
        out[key] = "<redacted>" if any(marker in key_l for marker in lowered) else value
    return out


def _is_sensitive(name: str, markers: list[str]) -> bool:
    text = name.lower()
    return any(marker in text for marker in markers)


def redact_json(value: Any, sensitive_markers: list[str], parent_key: str = "") -> Any:
    lowered = [item.lower() for item in sensitive_markers]
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, sub_value in value.items():
            if _is_sensitive(key, lowered):
                out[key] = "<redacted>"
            else:
                out[key] = redact_json(sub_value, lowered, parent_key=key)
        return out
    if isinstance(value, list):
        return [redact_json(item, lowered, parent_key=parent_key) for item in value]
    if isinstance(value, str) and _is_sensitive(parent_key, lowered):
        return "<redacted>"
    return value
