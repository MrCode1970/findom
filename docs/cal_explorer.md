# CAL Explorer Mode

## Назначение
Режим ручного исследования CAL Digital web:
- браузер с постоянным профилем (сессия сохраняется между запусками),
- полный capture request/response по целевым endpoint,
- авто-экстракция `result.transArr` в CSV/Markdown журнал.

## Запуск
Требование: установлен Playwright с Chromium.
```bash
pip install playwright
playwright install chromium
```

Переменные для автологина (из `.env` или env):
```bash
CAL_USERNAME=...
CAL_PASSWORD=...
GOOGLE_APPLICATION_CREDENTIALS=/home/vitaliy/mr1970code/findom/service_key.json
GOOGLE_SHEETS_ID=...
```

Запуск:
```bash
python tools/cal_explorer.py \
  --profile-dir /home/vitaliy/mr1970code/findom/.state/cal_digital/pw_profile \
  --headful \
  --keep-open \
  --log-dir /home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery
```

Live-запись в Google Sheets включена по умолчанию (`--live-sheet`).
Отключить можно через `--no-live-sheet`.

## Параметры
- `--profile-dir`: путь к persistent Chromium profile (`userDataDir`).
- `--headful`: запуск с GUI.
- `--keep-open`: не закрывать браузер автоматически, ждать `Ctrl+C`.
- `--log-dir`: каталог артефактов.
- `--url`: стартовый URL (по умолчанию `https://digital-web.cal-online.co.il/`).
- `--auto-login/--no-auto-login`: автозаполнение логина/пароля из `CAL_USERNAME`/`CAL_PASSWORD`.
- `--capture-contains`: фильтры URL через запятую для полного body-capture.
- `--live-sheet/--no-live-sheet`: включить/выключить live upsert в Google Sheets Journal.
- `--sheet-min-interval`: минимальный интервал между push в таблицу (сек).

Пример кастомного фильтра:
```bash
python tools/cal_explorer.py \
  --profile-dir /home/vitaliy/mr1970code/findom/.state/cal_digital/pw_profile \
  --headful --keep-open \
  --capture-contains "filteredTransactions,getFilteredTransactions,LastTransactionsForDashboard,account/init"
```

## Что сохраняется
Каталог: `/home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/`

- `network_log.full.jsonl`
  - request-события (xhr/fetch metadata),
  - response-события (status/headers + полный body inline или ссылка на файл).
- `responses/event_<id>.json|txt`
  - полные body для крупных/не-JSON ответов.
- `transactions_log.csv`
  - дедуп по ключу `cardUniqueId:trnIntId` (или `trnIntId`, если card нет).
- `transactions_log.md`
  - summary + последние 50 транзакций.

## Безопасность логов
- Редактируются чувствительные headers: `authorization`, `cookie`, `set-cookie`, `token`, `csrf`.
- Редактируются поля JSON: `authorization`, `tokenNumber`, `tokenInd`, `walletTokenInd`, `cookie`, `password`.
- Полные cookie в лог не записываются.

## Workflow ручного исследования
1. Запустить explorer в `--headful --keep-open`.
2. Войти в CAL (если сессии нет).
3. Открыть историю операций, менять фильтры, скроллить.
4. Проверять `transactions_log.md` и `network_log.full.jsonl`.
5. Завершить `Ctrl+C`.

## Мониторинг в реальном времени
Markdown-лог транзакций:
```bash
watch -n 2 "sed -n '1,120p' /home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/transactions_log.md"
```

JSONL-события сети:
```bash
tail -f /home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/network_log.full.jsonl
```

CSV-журнал транзакций:
```bash
tail -f /home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/transactions_log.csv
```

Проверить обновление в Google Sheets:
- лист `Journal` должен обновляться сразу после новых `result.transArr` в сети,
- в консоли explorer появляются строки вида `[sheet] Journal updated: <n> rows`.

## Happy path проверки
### 1) Первый запуск (без сессии)
- Удалить/переименовать профиль:
  - `/home/vitaliy/mr1970code/findom/.state/cal_digital/pw_profile`
- Запустить explorer.
- Вручную пройти логин.
- Убедиться, что заполняются:
  - `network_log.full.jsonl`,
  - `transactions_log.csv`,
  - `transactions_log.md`.

### 2) Повторный запуск (с сохраненной сессией)
- Запустить explorer с тем же `--profile-dir`.
- Проверить, что вход не требуется (или требуется реже, при истечении серверной сессии).
- Открыть операции и убедиться, что логи продолжают пополняться.

## Мини-аналитика по логу сети
```bash
python tools/analyze_network_log.py \
  --log /home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/network_log.full.jsonl \
  --out /home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/network_report.md
```
