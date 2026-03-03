from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


DEFAULT_LOG = Path("/home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/network_log.full.jsonl")
DEFAULT_REPORT = Path("/home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/network_report.md")
PAGING_KEYS = {"page", "size", "skip", "take", "offset", "limit", "pagesize", "pageindex"}


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise RuntimeError(f"Log file not found: {path}")
    items: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                payload["_line"] = line_no
                items.append(payload)
    return items


def _flatten_keys(value: Any, prefix: str = "") -> list[str]:
    keys: list[str] = []
    if isinstance(value, dict):
        for key, sub in value.items():
            full = f"{prefix}.{key}" if prefix else key
            keys.append(full)
            keys.extend(_flatten_keys(sub, full))
    elif isinstance(value, list):
        for idx, item in enumerate(value[:20]):
            full = f"{prefix}[{idx}]"
            keys.extend(_flatten_keys(item, full))
    return keys


def _iter_paging_hits(request_json: Any, query_params: dict[str, str]) -> list[str]:
    hits: list[str] = []
    for key in query_params.keys():
        if key.lower() in PAGING_KEYS:
            hits.append(f"query:{key}")
    for key_path in _flatten_keys(request_json):
        leaf = key_path.split(".")[-1]
        leaf = leaf.split("[", 1)[0].lower()
        if leaf in PAGING_KEYS:
            hits.append(f"json:{key_path}")
    return hits


def _parse_iso_date(value: str) -> datetime | None:
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_body_json_from_event(event: dict[str, Any], log_dir: Path) -> Any | None:
    response = event.get("response", {})
    if isinstance(response, dict) and "body_json" in response:
        return response.get("body_json")
    artifacts = event.get("artifacts", {})
    if not isinstance(artifacts, dict):
        return None
    body_file = artifacts.get("body_file")
    if not body_file:
        return None
    path = Path(body_file)
    if not path.is_absolute():
        path = log_dir / path
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def build_report(events: list[dict[str, Any]], log_path: Path) -> str:
    path_counter: Counter[str] = Counter()
    paging_counter: Counter[str] = Counter()

    caller_counter: Counter[str] = Counter()
    from_dates: list[datetime] = []
    to_dates: list[datetime] = []
    trans_sizes: list[int] = []

    for event in events:
        request = event.get("request", {})
        if not isinstance(request, dict):
            continue
        url = str(request.get("url", ""))
        if not url:
            continue
        parsed = urlparse(url)
        path_counter[parsed.path or "/"] += 1

        request_json = request.get("post_data_json")
        query_params = request.get("query_params", {})
        if not isinstance(query_params, dict):
            query_params = {}
        for hit in _iter_paging_hits(request_json, query_params):
            paging_counter[hit] += 1

        if "/Transactions/api/filteredTransactions/getFilteredTransactions" in url:
            if isinstance(request_json, dict):
                caller = str(request_json.get("caller", "")).strip()
                if caller:
                    caller_counter[caller] += 1
                from_dt = _parse_iso_date(str(request_json.get("fromTransDate", "")).strip())
                to_dt = _parse_iso_date(str(request_json.get("toTransDate", "")).strip())
                if from_dt is not None:
                    from_dates.append(from_dt)
                if to_dt is not None:
                    to_dates.append(to_dt)

            payload = _load_body_json_from_event(event, log_path.parent)
            if isinstance(payload, dict):
                result = payload.get("result")
                if isinstance(result, dict):
                    arr = result.get("transArr")
                    if isinstance(arr, list):
                        trans_sizes.append(len(arr))

    lines: list[str] = []
    lines.append("# Network Report")
    lines.append("")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Source log: {log_path}")
    lines.append(f"Total events: {len(events)}")
    lines.append("")

    lines.append("## Top paths")
    for path, count in path_counter.most_common(20):
        lines.append(f"- `{path}`: {count}")
    if not path_counter:
        lines.append("- no data")
    lines.append("")

    lines.append("## Paging params in request JSON/query")
    for key, count in paging_counter.most_common(20):
        lines.append(f"- `{key}`: {count}")
    if not paging_counter:
        lines.append("- no paging keys found")
    lines.append("")

    lines.append("## getFilteredTransactions")
    if caller_counter:
        lines.append("- caller frequencies:")
        for caller, count in caller_counter.most_common():
            lines.append(f"  - `{caller}`: {count}")
    else:
        lines.append("- caller frequencies: no data")

    if from_dates and to_dates:
        lines.append(
            f"- fromTransDate range: {min(from_dates).date().isoformat()} .. {max(from_dates).date().isoformat()}"
        )
        lines.append(
            f"- toTransDate range: {min(to_dates).date().isoformat()} .. {max(to_dates).date().isoformat()}"
        )
    else:
        lines.append("- date ranges: no data")

    if trans_sizes:
        lines.append(
            f"- transArr sizes: min={min(trans_sizes)}, max={max(trans_sizes)}, avg={sum(trans_sizes)/len(trans_sizes):.2f}"
        )
    else:
        lines.append("- transArr sizes: no data")
    lines.append("")

    return "\n".join(lines).strip() + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze full CAL network log and generate markdown report")
    parser.add_argument("--log", default=str(DEFAULT_LOG), help="Path to network_log.full.jsonl")
    parser.add_argument("--out", default=str(DEFAULT_REPORT), help="Path to output markdown report")
    args = parser.parse_args(argv)

    log_path = Path(args.log).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()

    try:
        events = _load_jsonl(log_path)
        report = build_report(events, log_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report, encoding="utf-8")
        print(f"OK: report saved to {out_path}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
