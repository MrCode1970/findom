# findom — CAL connector spike

## Быстрый старт (Linux Mint, Python 3.10+)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
python -m playwright install chromium
python -m findom
```

## Автоподстановка логина и пароля

`capture_har` умеет подставлять логин/пароль из ENV (или из файла `.env` в корне проекта).
Если открывается форма `כניסה מהירה`, скрипт пытается автоматически переключиться на `כניסה עם שם משתמש`.

Пример `.env`:

```bash
FINDOM_CAL_USERNAME=your_login
FINDOM_CAL_PASSWORD=your_password
```

После этого в меню запускайте обычный шаг захвата HAR (`[1]` или `[5]`).

## Основная команда

`python -m findom` запускает интерактивное меню с шагами:
- Захват HAR
- Извлечение из HAR
- Смоук-тест
- Полный пайплайн (1 -> 2 -> 3)
- Статус папки `out/`

## Ручные команды (advanced/manual)

```bash
python -m findom.cal.capture_har
python -m findom.cal.extract_har --har out/cal.har
python -m findom.cal.smoke --token-file out/token.txt --bundle-file out/token_bundle.json
```

## Что делает каждый шаг

- `capture_har`:
  - открывает Chromium и страницу `https://digital-web.cal-online.co.il/`
  - пишет сетевой HAR в `out/cal.har`
  - сохраняет storage state в `out/storage_state.json`
- `extract_har`:
  - ищет login endpoint и вытаскивает token в `out/token.txt`
  - находит пример запроса к transactions endpoint
  - пишет безопасную сводку в `out/har_summary.json`
  - пишет локальный bundle для smoke теста в `out/token_bundle.json`
- `smoke`:
  - берёт токен и bundle
  - делает POST к `getCardTransactionsDetails`
  - пишет ответ в `out/transactions_sample.json`

## Важные замечания

- Не выводите и не коммитьте секреты.
- Папка `out/` добавлена в `.gitignore`.
- Если в HAR нет тела ответа (или оно закодировано), `extract_har` попытается декодировать, а при неудаче подскажет причину в консоли.
