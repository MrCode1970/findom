from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class ArtifactStore:
    root_dir: Path
    run_id: str
    requests_path: Path = field(init=False)
    responses_path: Path = field(init=False)
    downloads_path: Path = field(init=False)
    run_log_path: Path = field(init=False)
    manifest_path: Path = field(init=False)
    summary_path: Path = field(init=False)

    def __post_init__(self) -> None:
        self.run_root = self.root_dir / self.run_id
        self.requests_path = self.run_root / "network_requests.jsonl"
        self.responses_path = self.run_root / "network_responses.jsonl"
        self.downloads_path = self.run_root / "downloads"
        self.run_log_path = self.run_root / "run.log"
        self.manifest_path = self.run_root / "reports_manifest.json"
        self.summary_path = self.run_root / "summary.json"

        self.downloads_path.mkdir(parents=True, exist_ok=True)
        self.requests_path.parent.mkdir(parents=True, exist_ok=True)

    def append_jsonl(self, path: Path, payload: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def log_line(self, line: str) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        with self.run_log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"[{timestamp}] {line}\n")

    def write_summary(self, payload: dict[str, Any]) -> None:
        self.summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def save_report_file(self, file_path: Path, report_type: str, account: str, period: str) -> dict[str, Any]:
        destination = self.downloads_path / file_path.name
        if file_path.resolve() != destination.resolve():
            shutil.copy2(file_path, destination)

        destination_str = str(destination)
        for existing in self.read_manifest():
            if existing.get("path") == destination_str:
                return existing

        record = {
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "report_type": report_type,
            "account": account,
            "period": period,
            "path": destination_str,
        }
        records = self.read_manifest()
        records.append(record)
        self.manifest_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
        return record

    def read_manifest(self) -> list[dict[str, Any]]:
        if not self.manifest_path.exists():
            return []
        try:
            data = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []
        return data if isinstance(data, list) else []
