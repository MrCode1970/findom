from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import requests


OUT_DIR = Path("out")
OUTPUT_PATH = OUT_DIR / "transactions_sample.json"



def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)



def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Smoke test call to CAL transactions endpoint")
    parser.add_argument("--token-file", required=True, help="Path to token.txt")
    parser.add_argument("--bundle-file", required=True, help="Path to token_bundle.json")
    args = parser.parse_args(argv)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    token = Path(args.token_file).read_text(encoding="utf-8").strip()
    if not token:
        raise RuntimeError("Token file is empty.")

    bundle = _load_json(Path(args.bundle_file))
    tx = bundle.get("transactions_request", {})

    method = str(tx.get("method", "POST")).upper()
    if method != "POST":
        raise RuntimeError(f"Unsupported method in bundle: {method}. Expected POST.")

    url = tx.get("url")
    if not isinstance(url, str) or not url:
        raise RuntimeError("Bundle does not contain transactions_request.url.")

    headers = tx.get("headers", {})
    if not isinstance(headers, dict):
        raise RuntimeError("Bundle transactions_request.headers must be an object.")

    payload = tx.get("json_body")
    if payload is None:
        raise RuntimeError("Bundle does not contain transactions_request.json_body.")

    request_headers = {str(k): str(v) for k, v in headers.items()}
    request_headers["Authorization"] = f"CALAuthScheme {token}"

    if "Accept" not in request_headers and "accept" not in {k.lower() for k in request_headers}:
        request_headers["Accept"] = "application/json, text/plain, */*"
    if "Content-Type" not in request_headers and "content-type" not in {k.lower() for k in request_headers}:
        request_headers["Content-Type"] = "application/json"

    response = requests.post(url, headers=request_headers, json=payload, timeout=60)

    try:
        body = response.json()
    except ValueError:
        body = {
            "_non_json_response": response.text,
        }

    result = {
        "status_code": response.status_code,
        "ok": response.ok,
        "response": body,
    }
    OUTPUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Smoke test завершён. Ответ сохранён: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
