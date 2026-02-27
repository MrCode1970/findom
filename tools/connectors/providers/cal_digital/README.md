# CAL Digital Provider

Production `requests-first` connector for CAL Digital.

## How it works

1. Loads endpoint hints from `.state/cal_digital/api_discovery/api_report.json`.
2. Builds `requests.Session` from `.state/cal_digital/api_discovery/storage_state.json`.
3. Runs API requests for cards and transactions.
4. If session is expired (401/403), automatically runs Playwright relogin via `discover_api` module and retries.

Playwright is used only for relogin/discovery refresh. Normal sync uses `requests` only.

## First-time setup

Generate API hints and browser session files once:

```bash
CAL_USERNAME=... CAL_PASSWORD=... python tools/connectors/providers/cal_digital/discover_api.py
```

## Run sync

```bash
CAL_USERNAME=... CAL_PASSWORD=... python tools/connectors/run_sync.py --provider cal_digital
```

Optional debug mode for relogin/discovery:

```bash
CAL_DEBUG=1 CAL_USERNAME=... CAL_PASSWORD=... python tools/connectors/run_sync.py --provider cal_digital
```

## State and files

- Connector state: `findom/.state/cal_digital.json`
  - `last_sync`
  - `storage_state_path`
  - `api_hints_cache`
- Discovery artifacts: `.state/cal_digital/api_discovery/`
  - `api_report.json`
  - `storage_state.json`
  - `network_log.jsonl`
  - other reports

## If session expired

No manual action is usually needed:

- provider validates the existing session via cards endpoint;
- on 401/403 it triggers relogin using `discover_api.run_discovery(...)`;
- refreshes `storage_state.json` and retries requests.

If relogin still fails, verify `CAL_USERNAME` / `CAL_PASSWORD` and inspect
`.state/cal_digital/api_discovery/diagnostics.json`.
