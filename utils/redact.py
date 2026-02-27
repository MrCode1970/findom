from __future__ import annotations

from typing import Any

SAFE_HEADER_NAMES = {"content-type", "accept", "origin", "referer"}



def redact_headers(header_map: dict[str, str]) -> dict[str, str]:
    redacted: dict[str, str] = {}
    for name, value in header_map.items():
        lower = name.lower()
        if lower in SAFE_HEADER_NAMES:
            redacted[name] = value
        elif "auth" in lower or "token" in lower or "cookie" in lower:
            redacted[name] = "<redacted>"
        else:
            redacted[name] = "<redacted>"
    return redacted



def redact_payload_shape(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: redact_payload_shape(v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_payload_shape(v) for v in value]
    return "<redacted>"
