# Idempotency Rules

## Movement external_id

`external_id` must be deterministic and generated from normalized movement fields:

1. Date part of `occurred_at` in ISO (`YYYY-MM-DD`).
2. Signed amount with two decimal digits.
3. `account_id`.
4. Cleaned `merchant`.
5. Cleaned `comment`.

Current implementation uses SHA-1 over a pipe-joined payload:

`sha1("date|amount|account_id|merchant|comment")`

This keeps identifiers stable across repeated sync runs if source data is unchanged.

## Upsert strategy

1. Upsert accounts by `(provider, account_id)`.
2. Upsert movements by `(provider, external_id)`.
3. Never mutate `external_id` once saved.
4. If provider supplies transfer pair markers, map them to `transfer_id` on both rows.

## Sync state

Each connector stores state at `findom/.state/<connector_name>.json`.
Minimal state field is `last_sync` in ISO date format.
