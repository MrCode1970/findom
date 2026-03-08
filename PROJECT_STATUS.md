# Project Status

## Overview
- Repository: `/home/vitaliy/mr1970code/findom`
- Goal: connectors platform + provider integrations + reporting surface
- Last updated: 2026-03-08

## Current State
- `cal_digital`: Ready
- `Google Sheets` showcase/vitrina: In progress
- `discount`: Ready (connector mode), stabilization in progress

## Completed
- Core connector contract and utilities (`_core`):
  - types, errors, storage, retry, http, normalize helpers
- Template provider skeleton with fixtures and tests
- `manual_csv` demonstration provider
- `cal_digital` discovery flow (Playwright) with API report artifacts
- `cal_digital` requests-first provider (`fetch -> api -> converters`)
- Runtime session reuse for stable requests replay
- `discount` provider (`fetch -> api -> converters`) with:
  - runtime request-template discovery from `.state/discount/research/*`,
  - relogin/discovery fallback to `.state/discount/api_discovery/storage_state.json`,
  - successful `tools/connectors/run_sync.py --provider discount` runs

## In Progress
- Google Sheets showcase schema and integration workflow polishing
- Progress/status tracking automation in repository docs
- CAL history profile stabilization for strict date windows and deterministic filtering

## Next
- Add explicit sync window CLI controls (`--from-date`, `--to-date`)
- Add lightweight run summaries for sync output
- Finalize and document stable CAL query profile (`filteredTransactions` + `getClearanceRequests`)

## Risks / Notes
- CAL private API may change UI/network behavior without notice
- Session/auth headers can expire and require fresh discovery
- For CAL and discount, stale runtime auth headers can produce `401`; reproducible replay requires fresh session
- Keep secrets only in environment (`.env`) and never in committed files
