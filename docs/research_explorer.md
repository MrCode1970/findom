# Unified research explorer

Единый механизм запускается через:

```bash
python tools/research/run_explorer.py --target <target_name>
```

Доступные target-конфиги:

- `cal_digital` — совместимость через legacy `tools/cal_explorer.py`.
- `discount` — первый общий сценарий в новом каркасе.

## Примеры

```bash
python tools/research/run_explorer.py --target cal_digital
python tools/research/run_explorer.py --target discount --headful --keep-open-seconds 60
```

## Артефакты

- Discount: `.state/discount/research/<run_id>/`
  - `network_requests.jsonl`
  - `network_responses.jsonl`
  - `storage_state.json`
  - `run.log`
  - `reports_manifest.json`
  - `summary.json`
  - `downloads/`

- CAL: используется существующий `tools/cal_explorer.py` и путь `.state/cal_digital/api_discovery/`.
