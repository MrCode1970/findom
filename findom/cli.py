from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

OUT_DIR = Path("out")
HAR_PATH = OUT_DIR / "cal.har"
TOKEN_PATH = OUT_DIR / "token.txt"
BUNDLE_PATH = OUT_DIR / "token_bundle.json"
SUMMARY_PATH = OUT_DIR / "har_summary.json"
SAMPLE_PATH = OUT_DIR / "transactions_sample.json"


def _print_menu() -> None:
    print("\n=== findom launcher ===")
    print("[1] Захват HAR")
    print("[2] Извлечение из HAR")
    print("[3] Смоук-тест")
    print("[4] Полный пайплайн (1 -> 2 -> 3)")
    print("[5] Повторный захват HAR для февраля (подсказки)")
    print("[6] Статус папки out/")
    print("[0] Выход")


def _extract_transactions_len(payload: dict[str, Any]) -> int | None:
    response = payload.get("response")
    if not isinstance(response, dict):
        return None

    result = response.get("result")
    if not isinstance(result, dict):
        return None

    bank_accounts = result.get("bankAccounts")
    if not isinstance(bank_accounts, list) or not bank_accounts:
        return None

    first_account = bank_accounts[0]
    if not isinstance(first_account, dict):
        return None

    debit_dates = first_account.get("debitDates")
    if not isinstance(debit_dates, list) or not debit_dates:
        return None

    first_debit_date = debit_dates[0]
    if not isinstance(first_debit_date, dict):
        return None

    transactions = first_debit_date.get("transactions")
    if not isinstance(transactions, list):
        return None

    return len(transactions)


def _action_capture() -> None:
    from findom.cal import capture_har

    print("\nОткроется браузер. Войдите в аккаунт, перейдите в транзакции, выберите карту и месяц.")
    print("Когда список транзакций загрузится, вернитесь в терминал и нажмите Enter.")
    capture_har.main([])


def _action_extract() -> None:
    from findom.cal import extract_har

    if not HAR_PATH.exists():
        print(f"\nФайл не найден: {HAR_PATH}. Сначала выполните шаг «Захват HAR».")
        return

    extract_har.main(["--har", str(HAR_PATH)])


def _action_smoke() -> None:
    from findom.cal import smoke

    missing_files = [path for path in [TOKEN_PATH, BUNDLE_PATH] if not path.exists()]
    if missing_files:
        missing_names = ", ".join(str(path) for path in missing_files)
        print(f"\nНе хватает файлов для смоук-теста: {missing_names}. Сначала выполните шаг «Извлечение из HAR».")
        return

    smoke.main(["--token-file", str(TOKEN_PATH), "--bundle-file", str(BUNDLE_PATH)])


def _action_full_pipeline() -> None:
    print("\nЗапуск полного пайплайна...")
    _action_capture()
    if not HAR_PATH.exists():
        print(f"Пайплайн остановлен: после захвата не найден {HAR_PATH}.")
        return

    _action_extract()
    if not TOKEN_PATH.exists() or not BUNDLE_PATH.exists():
        print("Пайплайн остановлен: отсутствуют token/bundle для смоук-теста.")
        return

    _action_smoke()


def _action_capture_february() -> None:
    from findom.cal import capture_har

    print("\nИнструкция для повторного захвата HAR (февраль):")
    print("1. После логина перейдите в раздел транзакций.")
    print("2. Переключите месяц на February.")
    print("3. Дождитесь, пока список транзакций станет непустым.")
    print("4. После этого нажмите Enter в терминале.")
    capture_har.main([])


def _action_out_status() -> None:
    print("\nСтатус папки out/:")
    if not OUT_DIR.exists():
        print("Папка out/ не существует.")
        return

    files = sorted(OUT_DIR.iterdir(), key=lambda p: p.name.lower())
    if not files:
        print("Папка out/ пуста.")
    else:
        for path in files:
            if path.is_file():
                size = path.stat().st_size
                print(f"- {path.name}: {size} байт")
            else:
                print(f"- {path.name}/")

    if not SAMPLE_PATH.exists():
        print("transactions: n/a (нет out/transactions_sample.json)")
        return

    try:
        payload = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    except Exception:
        print("transactions: n/a (не удалось прочитать out/transactions_sample.json)")
        return

    ok_flag = payload.get("ok")
    tx_len = _extract_transactions_len(payload)
    tx_label = "n/a" if tx_len is None else str(tx_len)
    print(f"ok: {ok_flag}")
    print(f"transactions: {tx_label}")


def main() -> None:
    actions: dict[str, Callable[[], None]] = {
        "1": _action_capture,
        "2": _action_extract,
        "3": _action_smoke,
        "4": _action_full_pipeline,
        "5": _action_capture_february,
        "6": _action_out_status,
    }

    while True:
        _print_menu()
        choice = input("Выберите действие: ").strip()

        if choice == "0":
            print("Выход.")
            return

        action = actions.get(choice)
        if action is None:
            print("Неизвестный пункт меню.")
            continue

        try:
            action()
        except Exception as exc:
            print(f"\nОшибка: {exc}")
            print(f"Подсказка: проверьте {SUMMARY_PATH} и файлы в папке out/.")
