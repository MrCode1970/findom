# Accounts Table Schema

One row represents one source account (card, checking, savings, cash wallet).

| Column | Type | Required | Description |
| --- | --- | --- | --- |
| provider | text | yes | Connector name (example: `manual_csv`). |
| account_id | text | yes | Stable account identifier inside provider namespace. |
| title | text | yes | Human-readable account name. |
| currency | text | yes | ISO 4217 code (for example `USD`, `EUR`, `RUB`). |
| balance | decimal(18,2) | no | Latest known account balance if provider sends it. |
| meta | json | no | Additional provider-specific attributes. |

Primary key recommendation: `(provider, account_id)`.
