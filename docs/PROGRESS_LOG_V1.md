# Progress Log V1

## Что уже готово
- `gsheets_toolkit` реализован:
  - `SheetsClient` (values + batch + clear + requests),
  - schema models (`ColumnSpec`, `SheetSchema`, `SpreadsheetSchema`),
  - sheet ops (`ensure_sheet`, `apply_sheet_schema`),
  - snapshot upsert (`load_key_index`, `upsert_rows_snapshot`).
- Лог-схема задана в [tables/log_schema.py](/home/vitaliy/mr1970code/findom/tables/log_schema.py):
  - `Journal` (A:I),
  - `_Meta` (A:B).
- Таблица лога создается скриптом [scripts/create_log_table.py](/home/vitaliy/mr1970code/findom/scripts/create_log_table.py).
- Writer лога реализован в [scripts/sync_log_to_sheet.py](/home/vitaliy/mr1970code/findom/scripts/sync_log_to_sheet.py):
  - `write_log_snapshot(spreadsheet_id, movements, synced_at_iso)`.
- Оркестратор источников и запуска:
  - [scripts/sources/registry.py](/home/vitaliy/mr1970code/findom/scripts/sources/registry.py),
  - [scripts/run_sync.py](/home/vitaliy/mr1970code/findom/scripts/run_sync.py).

## Аудит потоков данных
- `scripts/run_sync.py`:
  - парсит CLI (`--source`, `--from-date`, `--to-date`, `--write-log`, `--dry-run`);
  - формирует окно дат в локальном времени;
  - запускает `run_sources(...)`;
  - проверяет дубли `external_id`;
  - пишет лог только при `--write-log` и не при `--dry-run`;
  - печатает summary.
- `scripts/sources/registry.py`:
  - `cal_digital` запускается через фактические модули:
    - `tools.connectors.providers.cal_digital.api`
    - `tools.connectors.providers.cal_digital.converters`
  - `discount` пока `NotImplementedError`.
  - добавлен слой нормализации движений для лог-витрины (см. "Hardening").
- `scripts/sync_log_to_sheet.py`:
  - преобразует movement dict -> `Journal!A:I`,
  - upsert snapshot по ключу `H:external_id`,
  - обновляет `_Meta` одним `batchUpdateValues`.
- `gsheets_toolkit/upsert.py`:
  - читает индекс ключей по колонке,
  - группирует update-блоки последовательных строк,
  - append новых строк,
  - cleanup хвоста при уменьшении snapshot.

## Контракт лога (проверка)
- `1 movement = 1 row` в `Journal!A:I`.
- Обязательный ключ: `external_id` (колонка `H`).
- `date` должен быть ISO-строкой.
- `_Meta.row_count` = фактически записанные строки текущего snapshot.
- Snapshot-mode: после запуска в Journal остаются только данные текущего запуска (cleanup хвоста включен).

## Найденные риски и что сделано
- Риск: пустые/мусорные строки в Journal, когда movement приходит с `occurred_at`, а writer ожидает `date`.
  - Симптом: заполнен `external_id`, но `date` пустой.
  - Причина: несовпадение ключей между connector-output и writer-input.
- Сделано минимально-инвазивное hardening в `scripts/sources/registry.py`:
  - нормализация `occurred_at -> date`;
  - fail-fast по обязательным полям:
    - `date/occurred_at`, `amount`, `currency`, `account_id`, `source`, `external_id`;
  - валидация ISO для даты (`fromisoformat`).
- Коннекторы и `gsheets_toolkit` не изменялись.

## Артефакты CAL discovery
- [api_report.md](/home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/api_report.md)
- [api_report.json](/home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/api_report.json)
- [network_log.jsonl](/home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/network_log.jsonl)
- [requests_samples.py](/home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/requests_samples.py)
- [runtime_session.json](/home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/runtime_session.json)

## План: как быстрее выйти на “правильные” CAL запросы
- Цель: выбрать 1-2 endpoint, которые стабильно возвращают полный список транзакций за 30 дней.
- Базовые кандидаты:
  - `Authentication/api/account/init` (контекст),
  - `Transactions/api/filteredTransactions/getFilteredTransactions` (history),
  - `Transactions/api/LastTransactionsForDashboard/LastTransactionsForDashboard` (dashboard/smoke).

### Manual mode (рекомендуемый старт)
1. Открыть CAL UI и руками выставить фильтр периода (`30 days`) на экране операций.
2. В `network_log.jsonl` найти запрос `filteredTransactions/getFilteredTransactions` из этого действия.
3. Зафиксировать для него:
   - URL + method,
   - request headers (`authorization`, `x-site-id`, `content-type`, `referer`),
   - body (`fromTransDate`, `toTransDate`, `bankAccountUniqueID`, `cards[]`, `caller`, filter-поля).
4. Сравнить 2-3 последовательных запуска:
   - какие поля динамические,
   - где paging (`page/offset/limit`) и есть ли truncation.
5. Проверить воспроизводимость тем же session/cookies:
   - использовать `runtime_session.json` + `requests_samples.py` как baseline.
6. Зафиксировать “рабочий минимальный body”:
   - оставить обязательный набор,
   - убрать лишние фильтры, но не ломать выдачу.

### Что искать в логах
- URL path содержит:
  - `filteredTransactions/getFilteredTransactions`,
  - при необходимости `LastTransactionsForDashboard`.
- Headers:
  - `authorization`,
  - `x-site-id`,
  - `content-type`,
  - `referer`,
  - опционально специфичные `x-*`.
- Body:
  - `fromTransDate`, `toTransDate`,
  - `bankAccountUniqueID`,
  - `cards` / `cardUniqueID`,
  - параметры пагинации/сортировки.
- Response:
  - где массив операций (`result.transArr` и размер массива),
  - индикаторы продолжения страниц (если есть).

## Команды диагностики
- Создать/применить структуру таблицы:
```bash
python scripts/create_log_table.py
```

- Fixture sync (диагностический writer):
```bash
python scripts/sync_log_to_sheet.py
```

- Оркестратор dry-run (без записи в Sheets):
```bash
python scripts/run_sync.py --source cal_digital --dry-run
```

- Оркестратор с записью snapshot:
```bash
python scripts/run_sync.py --source cal_digital --write-log
```

## Проверка Journal/_Meta
- `Journal`:
  - заголовки `A:I`,
  - отсутствуют пустые `date` при непустом `external_id`,
  - нет дублей `external_id` за один запуск.
- `_Meta`:
  - `last_sync_at` обновлен,
  - `row_count` = число строк текущего snapshot,
  - `schema_version = 1`.

## Известные проблемы/гипотезы
- Иногда мало `movements`:
  - узкое окно синка,
  - фильтры CAL endpoint,
  - не все карты/статусы включены в body.
- Потенциальные пропуски:
  - необходимость paging или дополнительных флагов в `filteredTransactions`.
- Для старта допустим ручной режим фиксации рабочего запроса из `network_log.jsonl`, затем стабилизация в коде.

