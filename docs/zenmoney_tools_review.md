# ZenMoney / ZenPlugins Tools Review (для findom)

## Источники анализа (локальные файлы)
- `/home/vitaliy/mr1970code/findom/zenmoney/ZenPlugins/docs/guidelines.md`
- `/home/vitaliy/mr1970code/findom/zenmoney/ZenPlugins/docs/utils.md`
- `/home/vitaliy/mr1970code/findom/zenmoney/ZenPlugins/src/common/network.d.ts`
- `/home/vitaliy/mr1970code/findom/zenmoney/ZenPlugins/src/common/network.js`
- `/home/vitaliy/mr1970code/findom/zenmoney/ZenPlugins/src/types/zenmoney.ts`
- `/home/vitaliy/mr1970code/findom/zenmoney/ZenPlugins/src/plugins/example/{fetchApi.ts,api.ts,converters.ts}`
- примеры `preferences.xml` и usage `ZenMoney.getData/setData/saveData` в плагинах (`inecobank-am`, `skbbank`, `halyk`, `alfabank-by`, `apelsin-uz` и др.)

## Что можно напрямую использовать в нашем кейсе
1. Архитектурный паттерн `fetchApi -> api -> converters`
- В ZenPlugins это явно зафиксировано в `guidelines.md`.
- Для findom это уже применено и стоит продолжать:
  - `fetch` = 1 endpoint/функция,
  - `api` = flow/оркестрация и retries,
  - `converters` = строгая нормализация в доменный контракт.

2. Сетевые практики
- В `src/common/network.js`:
  - унифицированный fetch/fetchJson,
  - логирование request/response,
  - маскирование чувствительных данных.
- Для findom полезно как reference по:
  - централизации HTTP клиента,
  - parse/sanitize policy,
  - стандартизации ошибок парсинга (`ParseError`).

3. WebView/intercept паттерн
- `openWebViewAndInterceptRequest` в `network.d.ts/.js`:
  - intercept запросов в auth-flow,
  - извлечение токенов/кодов без полного API reverse.
- В findom прямого WebView API нет, но идея эквивалентна:
  - Playwright + intercept network,
  - persistent profile + runtime_session artifacts.

4. Persistent storage сессии
- В ZenPlugins типичный паттерн:
  - `ZenMoney.getData/setData/saveData` для device/auth/cookies.
- Для findom применимо через локальные файлы:
  - `.state/.../storage_state.json`,
  - `.state/.../runtime_session.json`,
  - connector state (`last_sync` и прочее).

5. Контракт транзакций и converter tests
- В `src/types/zenmoney.ts` и многочисленных `__tests__`:
  - строгий domain contract,
  - высокая плотность тестов конвертеров.
- Для findom стоит продолжать:
  - тестировать tricky кейсы знака суммы, transfer/refund, dedup.

## Что НЕ применимо напрямую
1. Глобальный API ZenMoney
- `ZenMoney.*` (`openWebView`, `getData`, `saveData`, `readLine`, feature flags) специфичен для runtime приложения ZenMoney.
- В findom это нельзя использовать напрямую; нужен эквивалент на Python/Playwright/filesystem.

2. Манифесты/настройки плагинов
- `preferences.xml`, `ZenmoneyManifest.xml` и связанный pipeline сборки относятся к экосистеме ZenPlugins.
- Для findom это не нужно напрямую; полезно только как reference для конфигурационных принципов (`startDate`, required preferences).

3. TypeScript-specific tooling
- Встроенные типы/утилиты TS (например `get.ts`, compile-time checks) нельзя перенести 1-в-1.
- В findom роль типобезопасности выполняют dataclass/pydantic/tests.

## Рекомендованный минимальный набор идей для копирования в findom
1. Слои:
- сохранить и закрепить `fetch -> api -> converters` как стандарт для всех новых провайдеров.

2. Состояние:
- хранить session/token/runtime hints в `.state` с четкой схемой и versioning.

3. Ошибки:
- централизовать типы ошибок auth/rate-limit/temp/captcha и retry policy.

4. Capture/Debug:
- стандартизировать исследовательский режим (как `tools/cal_explorer.py`) для ручного reverse API.

5. Тестовый каркас:
- обязательные converter tests на:
  - sign logic,
  - stable idempotency keys,
  - missing fields robustness,
  - transfer/refund cases.

## Для задачи CAL (авторизация, history, pagination)
- Полезно брать из Zen-подхода:
  - flow в `api` (login/check session/fetch pages),
  - fetch wrappers + parse guards,
  - converter-first thinking + regression tests.
- Не переносить:
  - `ZenMoney.openWebViewAndInterceptRequest` напрямую.
  - Вместо этого использовать текущий Playwright explorer + persistent profile.

## Итог
- ZenPlugins дает сильный архитектурный шаблон и практики тестируемости.
- Для findom разумная стратегия: копировать **идеи/структуру**, не платформенные API.
- Текущее состояние findom уже близко к этому подходу; следующий шаг — систематизировать pagination/history извлечение CAL через explorer logs и точечные fetch-правки.

