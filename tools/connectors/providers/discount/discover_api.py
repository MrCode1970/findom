from __future__ import annotations

import json
import os
from pathlib import Path

from playwright.sync_api import sync_playwright


DEFAULT_LOGIN_URL = "https://start.telebank.co.il/login/#/LOGIN_PAGE"
REPO_ROOT = Path(__file__).resolve().parents[4]
DISCOVERY_ROOT = REPO_ROOT / ".state" / "discount" / "api_discovery"
PROFILE_DIR = DISCOVERY_ROOT / "pw_profile"
STORAGE_STATE_PATH = DISCOVERY_ROOT / "storage_state.json"
DIAGNOSTICS_PATH = DISCOVERY_ROOT / "diagnostics.json"


def _write_diagnostics(payload: dict) -> None:
    DISCOVERY_ROOT.mkdir(parents=True, exist_ok=True)
    DIAGNOSTICS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _credentials() -> tuple[str, str, str]:
    username = (os.getenv("DISCOUNT_USERNAME") or "").strip()
    password = (os.getenv("DISCOUNT_PASSWORD") or "").strip()
    code = (os.getenv("DISCOUNT_CODE") or "").strip()
    if not username or not password or not code:
        raise RuntimeError("DISCOUNT_USERNAME/DISCOUNT_PASSWORD/DISCOUNT_CODE are required")
    return username, password, code


def run_discovery(*, base_url: str = DEFAULT_LOGIN_URL, debug: bool = False) -> int:
    try:
        username, password, code = _credentials()
    except Exception as exc:
        _write_diagnostics({"status": "failed", "reason": str(exc)})
        return 1

    DISCOVERY_ROOT.mkdir(parents=True, exist_ok=True)
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            str(PROFILE_DIR),
            headless=not debug,
            accept_downloads=False,
        )
        page = context.pages[0] if context.pages else context.new_page()

        try:
            page.goto(base_url, wait_until="domcontentloaded")
            page.locator("input#tzId").first.fill(username)
            page.locator("input#tzPassword").first.fill(password)
            page.locator("input#aidnum").first.fill(code)

            try:
                page.locator("button[type='submit']").first.click(timeout=3000)
            except Exception:
                page.keyboard.press("Enter")

            page.wait_for_timeout(8000)
            context.storage_state(path=str(STORAGE_STATE_PATH))

            _write_diagnostics(
                {
                    "status": "ok",
                    "storage_state_path": str(STORAGE_STATE_PATH),
                    "final_url": page.url,
                    "headless": not debug,
                }
            )
            return 0
        except Exception as exc:
            _write_diagnostics(
                {
                    "status": "failed",
                    "reason": str(exc),
                    "final_url": page.url,
                    "headless": not debug,
                }
            )
            return 1
        finally:
            context.close()
