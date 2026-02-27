# Project Status

## Overview
- Repository: `/home/vitaliy/mr1970code/findom`
- Goal: connectors platform + provider integrations + reporting surface
- Last updated: 2026-02-27

## Current State
- `cal_digital`: Ready
- `Google Sheets` showcase/vitrina: In progress
- `Discount` provider: Next

## Completed
- Core connector contract and utilities (`_core`):
  - types, errors, storage, retry, http, normalize helpers
- Template provider skeleton with fixtures and tests
- `manual_csv` demonstration provider
- `cal_digital` discovery flow (Playwright) with API report artifacts
- `cal_digital` requests-first provider (`fetch -> api -> converters`)
- Runtime session reuse for stable requests replay

## In Progress
- Google Sheets showcase schema and integration workflow polishing
- Progress/status tracking automation in repository docs

## Next
- Discount provider bootstrap with same connector pattern
- Add explicit sync window CLI controls (`--from-date`, `--to-date`)
- Add lightweight run summaries for sync output

## Risks / Notes
- CAL private API may change UI/network behavior without notice
- Session/auth headers can expire and require fresh discovery
- Keep secrets only in environment (`.env`) and never in committed files

