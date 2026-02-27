from __future__ import annotations

import base64
import gzip
import json
from pathlib import Path
from typing import Any



def load_har(path: str | Path) -> dict[str, Any]:
    har_path = Path(path)
    with har_path.open("r", encoding="utf-8") as f:
        return json.load(f)



def entries_from_har(har: dict[str, Any]) -> list[dict[str, Any]]:
    return har.get("log", {}).get("entries", [])



def header_list_to_dict(headers: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in headers or []:
        name = str(item.get("name", ""))
        value = str(item.get("value", ""))
        if name:
            out[name] = value
    return out



def try_parse_json(text: str | None) -> Any | None:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None



def decode_har_content_text(content: dict[str, Any]) -> str | None:
    text = content.get("text")
    if text is None:
        return None

    encoding = content.get("encoding")
    try:
        if encoding == "base64":
            raw = base64.b64decode(text)
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return raw.decode("utf-8", errors="replace")
    except Exception:
        return None

    if isinstance(text, str):
        return text
    return str(text)



def request_post_data_text(entry: dict[str, Any]) -> str | None:
    request = entry.get("request", {})
    post_data = request.get("postData", {})
    text = post_data.get("text")
    if text is None:
        return None
    return str(text)



def response_text(entry: dict[str, Any]) -> str | None:
    response = entry.get("response", {})
    content = response.get("content", {})
    return decode_har_content_text(content)



def find_entries_by_url_substring(
    entries: list[dict[str, Any]],
    url_substring: str,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for entry in entries:
        request = entry.get("request", {})
        url = str(request.get("url", ""))
        if url_substring in url:
            matches.append(entry)
    return matches



def pick_headers_for_smoke(header_map: dict[str, str]) -> dict[str, str]:
    selected: dict[str, str] = {}
    for name, value in header_map.items():
        lower = name.lower()
        if lower in {"authorization", "cookie", "host", "content-length", "connection"}:
            continue
        if lower.startswith("sec-"):
            continue
        if lower in {"content-type", "accept", "origin", "referer"} or lower.startswith("x-"):
            selected[name] = value
    return selected
