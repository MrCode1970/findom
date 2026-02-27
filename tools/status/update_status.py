from __future__ import annotations

import argparse
import subprocess
from datetime import date
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CHANGELOG_PATH = REPO_ROOT / "CHANGELOG.md"
PROJECT_STATUS_PATH = REPO_ROOT / "PROJECT_STATUS.md"


def _ensure_changelog_exists() -> None:
    if CHANGELOG_PATH.exists():
        return
    CHANGELOG_PATH.write_text("# Changelog\n\n", encoding="utf-8")


def _last_bullet_line(text: str) -> str | None:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in reversed(lines):
        if line.startswith("- "):
            return line
    return None


def append_changelog(entry: str) -> bool:
    """Append a changelog bullet if it is not a duplicate of the last bullet.

    Returns True when entry was appended, False when skipped.
    """
    normalized = entry.strip()
    if not normalized:
        return False
    bullet = f"- {normalized}"

    _ensure_changelog_exists()
    current = CHANGELOG_PATH.read_text(encoding="utf-8")
    last_bullet = _last_bullet_line(current)
    if last_bullet == bullet:
        return False

    today_header = f"## {date.today().isoformat()}"
    if today_header in current:
        new_text = current.rstrip() + "\n" + bullet + "\n"
    else:
        sep = "" if current.endswith("\n\n") else ("\n" if current.endswith("\n") else "\n\n")
        new_text = current + sep + today_header + "\n" + bullet + "\n"

    CHANGELOG_PATH.write_text(new_text, encoding="utf-8")
    return True


def _read_recent_git_subjects(n_commits: int) -> list[str]:
    cmd = ["git", "log", f"-n{n_commits}", "--pretty=format:%s"]
    try:
        result = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        return []
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return lines


def auto_log_from_git(n_commits: int = 5) -> int:
    """Read recent git log subjects and append a compact changelog entry."""
    subjects = _read_recent_git_subjects(n_commits=n_commits)
    if not subjects:
        return 0

    latest = subjects[0]
    entry = f"Auto-log from git ({n_commits}): {latest}"
    return int(append_changelog(entry))


def _status_lines() -> list[str]:
    if not PROJECT_STATUS_PATH.exists():
        return [
            "PROJECT_STATUS.md: missing",
            "CHANGELOG.md: " + ("present" if CHANGELOG_PATH.exists() else "missing"),
        ]

    content = PROJECT_STATUS_PATH.read_text(encoding="utf-8").splitlines()
    summary = [line.strip() for line in content if line.strip().startswith("- `") or line.strip().startswith("- ")]
    out = ["PROJECT_STATUS.md: present", "CHANGELOG.md: " + ("present" if CHANGELOG_PATH.exists() else "missing")]
    out.extend(summary[:6])
    return out


def print_status_summary() -> None:
    for line in _status_lines():
        print(line)


def main() -> int:
    parser = argparse.ArgumentParser(description="Update project status/changelog helpers")
    parser.add_argument("--auto", action="store_true", help="Append one auto-log entry from recent git commits")
    parser.add_argument("--entry", type=str, default="", help="Append a custom changelog entry")
    parser.add_argument("--summary", action="store_true", help="Print status summary")
    parser.add_argument("--commits", type=int, default=5, help="How many recent commits to inspect for --auto")
    args = parser.parse_args()

    changed = 0
    if args.entry:
        changed += int(append_changelog(args.entry))
    if args.auto:
        changed += auto_log_from_git(n_commits=max(1, args.commits))
    if args.summary or (not args.entry and not args.auto):
        print_status_summary()
    return 0 if changed >= 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

