from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from playwright.sync_api import BrowserContext, Page, sync_playwright

from .artifacts import ArtifactStore
from .config import MechanismConfig, TargetConfig
from .redaction import redact_headers, redact_json


@dataclass
class RunStats:
    provider: str
    started_at: str
    requests: int = 0
    responses: int = 0
    downloads: int = 0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_root(target: TargetConfig) -> Path:
    out = target.artifacts.get("root") or ".state/research"
    return Path(out)


def _collect_env(target: TargetConfig) -> dict[str, str]:
    names = target.env.get("required", [])
    result: dict[str, str] = {}
    for name in names:
        value = os.getenv(name, "")
        if value:
            result[name] = value
    return result


def _run_legacy_if_requested(target: TargetConfig, headful: bool) -> bool:
    legacy = target.compatibility.get("legacy_command")
    if not legacy:
        return False
    env = os.environ.copy()
    if headful:
        env["PWDEBUG"] = env.get("PWDEBUG", "1")
    subprocess.run(legacy, shell=True, check=True, env=env)
    return True


def _build_context(playwright: Any, mechanism: MechanismConfig, target: TargetConfig, profile_dir: Path) -> BrowserContext:
    browser_cfg = mechanism.browser
    persistent = bool(browser_cfg.get("persistent_profile", True))
    headless = bool(browser_cfg.get("headless", False))

    if persistent:
        profile_dir.mkdir(parents=True, exist_ok=True)
        return playwright.chromium.launch_persistent_context(
            str(profile_dir),
            headless=headless,
            accept_downloads=True,
        )

    browser = playwright.chromium.launch(headless=headless)
    return browser.new_context(accept_downloads=True)


def _apply_login(page: Page, target: TargetConfig, env_values: dict[str, str], store: ArtifactStore) -> None:
    login = target.ui.get("login", {})
    fields = login.get("fields", [])
    submit_selector = login.get("submit_selector")

    for item in fields:
        selector = item.get("selector")
        env_key = item.get("env")
        if not selector or not env_key:
            continue
        value = env_values.get(env_key)
        if value is None:
            continue
        try:
            page.locator(selector).first.fill(value)
            store.log_line(f"Filled login field for env '{env_key}'")
        except Exception as exc:
            store.log_line(f"Login field fill skipped for env '{env_key}': {exc}")

    if submit_selector:
        try:
            page.locator(submit_selector).first.click()
            store.log_line("Login submit clicked")
        except Exception as exc:
            store.log_line(f"Login submit skipped: {exc}")


def _attach_capture(context: BrowserContext, mechanism: MechanismConfig, store: ArtifactStore, stats: RunStats) -> None:
    capture_cfg = mechanism.capture
    headers_markers = mechanism.redaction.get("header_markers", [])
    body_markers = mechanism.redaction.get("body_markers", [])
    include_response_body = bool(capture_cfg.get("include_response_body", True))

    def on_request(request: Any) -> None:
        try:
            stats.requests += 1
            body_payload: Any = None
            if request.post_data:
                try:
                    body_payload = redact_json(json.loads(request.post_data), body_markers)
                except Exception:
                    body_payload = "<unparsed>"

            store.append_jsonl(
                store.requests_path,
                {
                    "ts": _utc_now(),
                    "method": request.method,
                    "url": request.url,
                    "resource_type": request.resource_type,
                    "headers": redact_headers(dict(request.headers), headers_markers),
                    "body": body_payload,
                },
            )
        except BaseException as exc:
            store.log_line(f"Request capture skipped: {exc}")

    def on_response(response: Any) -> None:
        try:
            stats.responses += 1
            payload: dict[str, Any] = {
                "ts": _utc_now(),
                "url": response.url,
                "status": response.status,
                "headers": redact_headers(dict(response.headers), headers_markers),
            }
            if include_response_body:
                try:
                    text = response.text()
                    if len(text) <= int(capture_cfg.get("inline_body_max_chars", 200000)):
                        try:
                            payload["body"] = redact_json(json.loads(text), body_markers)
                        except Exception:
                            payload["body"] = text[:5000]
                    else:
                        payload["body"] = "<omitted_too_large>"
                except BaseException:
                    payload["body"] = "<unavailable>"

            store.append_jsonl(store.responses_path, payload)
        except BaseException as exc:
            store.log_line(f"Response capture skipped: {exc}")

    context.on("request", on_request)
    context.on("response", on_response)


def _attach_download_capture(
    context: BrowserContext,
    store: ArtifactStore,
    stats: RunStats,
    download_cfg: dict[str, Any],
) -> None:
    def on_download(download: Any) -> None:
        try:
            filename = download.suggested_filename or f"download_{int(datetime.now(timezone.utc).timestamp())}"
            destination = store.downloads_path / filename
            download.save_as(str(destination))
            account = download_cfg.get("default_account", "unknown")
            period = download_cfg.get("default_period", "manual")
            report_type = download_cfg.get("default_report_type", "statement")
            store.save_report_file(destination, report_type=report_type, account=account, period=period)
            stats.downloads += 1
            store.log_line(f"Captured download: {destination}")
        except BaseException as exc:
            store.log_line(f"Download capture skipped: {exc}")

    def attach_page(page: Page) -> None:
        try:
            page.on("download", on_download)
            store.log_line(f"Download listener attached: {page.url or '<blank>'}")
        except BaseException as exc:
            store.log_line(f"Download listener attach skipped: {exc}")

    for page in context.pages:
        attach_page(page)
    context.on("page", attach_page)


def _start_urls(target: TargetConfig) -> list[str]:
    urls = [target.start_url]
    for candidate in target.start_urls:
        if candidate and candidate not in urls:
            urls.append(candidate)
    return urls


def _is_usable_page_url(url: str) -> bool:
    return bool(url) and url.startswith("http") and not url.startswith("chrome-error://")


def _open_start_page(page: Page, target: TargetConfig, store: ArtifactStore) -> str:
    failures: list[str] = []
    for url in _start_urls(target):
        try:
            page.goto(url, wait_until="domcontentloaded")
            store.log_line(f"Opened start URL: {url}")
            return url
        except Exception as exc:
            current_url = ""
            try:
                page.wait_for_timeout(500)
                current_url = page.url
            except Exception:
                current_url = ""
            if _is_usable_page_url(current_url):
                store.log_line(
                    f"Start URL recovered after navigation interruption: requested={url}, current={current_url}"
                )
                return current_url
            message = f"{url} -> {exc}"
            failures.append(message)
            store.log_line(f"Start URL failed: {message}")
    joined = " | ".join(failures) if failures else "no URLs configured"
    raise RuntimeError(f"Unable to open any start URL: {joined}")


def run_target_exploration(
    mechanism: MechanismConfig,
    target: TargetConfig,
    *,
    headful: bool,
    keep_open: bool,
    keep_open_seconds: int,
) -> int:
    if target.compatibility.get("mode") == "legacy_cal_explorer":
        return 0 if _run_legacy_if_requested(target, headful=headful) else 1

    env_values = _collect_env(target)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    root = _resolve_root(target)
    store = ArtifactStore(root_dir=root, run_id=run_id)
    stats = RunStats(provider=target.provider_name, started_at=_utc_now())
    print(f"[research] artifacts: {store.run_root}")

    missing = [name for name in target.env.get("required", []) if name not in env_values]
    if missing:
        store.log_line(f"Missing env vars: {', '.join(missing)}")

    profile_dir = root / "pw_profile"
    interrupted = False
    failure: str | None = None

    with sync_playwright() as pw:
        context = _build_context(pw, mechanism, target, profile_dir=profile_dir)
        _attach_capture(context, mechanism, store, stats)
        download_cfg = target.reports.get("download_capture", {})
        capture_downloads = bool(download_cfg.get("enabled", True))

        if capture_downloads:
            _attach_download_capture(context, store, stats, download_cfg)

        try:
            page = context.pages[0] if context.pages else context.new_page()
            _open_start_page(page, target, store)

            _apply_login(page, target, env_values, store)

            wait_after_login = int(target.ui.get("post_login_wait_seconds", 10))
            page.wait_for_timeout(wait_after_login * 1000)

            if keep_open:
                store.log_line("Keep-open mode enabled; waiting until Ctrl+C")
                while True:
                    page.wait_for_timeout(1000)
            elif keep_open_seconds > 0:
                page.wait_for_timeout(keep_open_seconds * 1000)
        except KeyboardInterrupt:
            interrupted = True
            store.log_line("Interrupted by user (Ctrl+C)")
        except Exception as exc:
            failure = str(exc)
            store.log_line(f"Run failed: {exc}")
        finally:
            try:
                context.storage_state(path=str(store.run_root / "storage_state.json"))
            except BaseException as exc:
                store.log_line(f"storage_state save skipped: {exc}")
            context.close()

    store.write_summary(
        {
            "provider": stats.provider,
            "started_at": stats.started_at,
            "finished_at": _utc_now(),
            "requests": stats.requests,
            "responses": stats.responses,
            "downloads": len(store.read_manifest()),
            "interrupted": interrupted,
            "failure": failure,
            "target_sections": target.ui.get("important_sections", []),
            "endpoint_hints": target.endpoint_hints,
        }
    )
    store.log_line("Run finished")
    if interrupted:
        return 130
    if failure:
        return 1
    return 0
