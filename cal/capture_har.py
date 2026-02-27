from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable

from playwright.sync_api import sync_playwright


OUT_DIR = Path("out")
HAR_PATH = OUT_DIR / "cal.har"
STORAGE_STATE_PATH = OUT_DIR / "storage_state.json"
TARGET_URL = "https://digital-web.cal-online.co.il/"
DEFAULT_DOTENV_PATH = ".env"
DEFAULT_USERNAME_ENV = "FINDOM_CAL_USERNAME"
DEFAULT_PASSWORD_ENV = "FINDOM_CAL_PASSWORD"
FALLBACK_USERNAME_ENVS = ("CAL_USERNAME",)
FALLBACK_PASSWORD_ENVS = ("CAL_PASSWORD",)
LOGIN_PROMPT = (
    "Войдите в аккаунт в открытом окне браузера, затем перейдите в транзакции, "
    "выберите карту/месяц и дождитесь загрузки списка. После этого нажмите Enter в терминале."
)


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _load_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    env_values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        env_values[key] = _strip_quotes(value.strip())
    return env_values


def _dotenv_candidates(dotenv_path: str) -> list[Path]:
    path = Path(dotenv_path).expanduser()
    if path.is_absolute():
        return [path]

    cwd_path = Path.cwd() / path
    project_root_path = Path(__file__).resolve().parents[1] / path
    if cwd_path == project_root_path:
        return [cwd_path]
    return [cwd_path, project_root_path]


def _read_credential(
    names: Iterable[str],
    dotenv_values: dict[str, str],
) -> tuple[str | None, str | None, str | None]:
    for name in names:
        env_value = os.environ.get(name)
        if env_value:
            return env_value, "env", name

    for name in names:
        dotenv_value = dotenv_values.get(name)
        if dotenv_value:
            return dotenv_value, "dotenv", name
    return None, None, None


def _try_fill_first(page, selectors: Iterable[str], value: str) -> bool:
    for selector in selectors:
        locator = page.locator(selector)
        try:
            count = locator.count()
        except Exception:
            continue

        for idx in range(count):
            field = locator.nth(idx)
            if _fill_and_verify(field, value):
                return True
    return False


def _login_targets(page) -> list:
    targets = [page]
    for frame in page.frames:
        if frame != page.main_frame:
            targets.append(frame)
    return targets


def _click_first_clickable(locator) -> bool:
    try:
        count = locator.count()
    except Exception:
        return False

    for idx in range(min(count, 10)):
        candidate = locator.nth(idx)
        try:
            candidate.click(timeout=1200)
            return True
        except Exception:
            continue
    return False


def _switch_via_js(target, needle: str) -> bool:
    script = """
    (needle) => {
      const selectors = ["button", "a", "[role='button']", "[role='tab']", "label", "div", "span"];
      const nodes = Array.from(document.querySelectorAll(selectors.join(",")));
      for (const el of nodes) {
        const text = (el.textContent || "").trim();
        if (!text.includes(needle)) continue;
        el.click();
        return true;
      }
      return false;
    }
    """
    try:
        return bool(target.evaluate(script, needle))
    except Exception:
        return False


def _has_non_empty_value(field) -> bool:
    try:
        current = field.input_value()
        return bool(current.strip())
    except Exception:
        return False


def _fill_and_verify(field, value: str) -> bool:
    try:
        field.fill(value, timeout=1200)
        if _has_non_empty_value(field):
            return True
    except Exception:
        pass

    try:
        field.click(timeout=1200)
        field.press("ControlOrMeta+a", timeout=1200)
        field.type(value, delay=35, timeout=2500)
        if _has_non_empty_value(field):
            return True
    except Exception:
        pass

    try:
        field.evaluate(
            """(el, val) => {
                const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
                if (setter) {
                  setter.call(el, val);
                } else {
                  el.value = val;
                }
                el.dispatchEvent(new Event("input", { bubbles: true }));
                el.dispatchEvent(new Event("change", { bubbles: true }));
                el.dispatchEvent(new Event("blur", { bubbles: true }));
            }""",
            value,
        )
        if _has_non_empty_value(field):
            return True
    except Exception:
        pass

    return False


def _fill_via_js_fallback(target, value: str, is_password: bool) -> bool:
    script = """
    ({ value, isPassword }) => {
      const lower = (s) => (s || "").toLowerCase();
      const has = (s, parts) => parts.some((p) => lower(s).includes(p));
      const visible = (el) => !!(el.offsetParent || el.getClientRects().length);
      const inputs = Array.from(document.querySelectorAll("input"))
        .filter((el) => !el.disabled && !el.readOnly && visible(el));

      const userWords = ["user", "login", "id", "userid", "name", "שם", "תעודת", "זהות", "tz"];
      const passWords = ["pass", "password", "pwd", "סיסמ", "קוד"];
      const candidates = inputs.filter((el) => {
        const type = lower(el.getAttribute("type"));
        if (isPassword) {
          return type === "password" || has(el.name, passWords) || has(el.id, passWords) || has(el.placeholder, passWords) || has(el.getAttribute("aria-label"), passWords);
        }
        return type !== "password";
      });
      if (!candidates.length) return false;

      let best = null;
      let bestScore = -1;
      for (const el of candidates) {
        let score = 0;
        const type = lower(el.getAttribute("type"));
        const key = `${el.name || ""} ${el.id || ""} ${el.placeholder || ""} ${el.getAttribute("aria-label") || ""}`;
        if (isPassword) {
          if (type === "password") score += 5;
          if (has(key, passWords)) score += 4;
        } else {
          if (type === "text" || type === "email" || type === "tel" || type === "number" || !type) score += 2;
          if (has(key, userWords)) score += 5;
        }
        if (score > bestScore) {
          bestScore = score;
          best = el;
        }
      }
      if (!best) best = candidates[0];

      const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
      if (setter) setter.call(best, value);
      else best.value = value;
      best.dispatchEvent(new Event("input", { bubbles: true }));
      best.dispatchEvent(new Event("change", { bubbles: true }));
      best.dispatchEvent(new Event("blur", { bubbles: true }));
      return !!String(best.value || "").trim();
    }
    """
    try:
        return bool(target.evaluate(script, {"value": value, "isPassword": is_password}))
    except Exception:
        return False


def _switch_to_username_login(page) -> bool:
    switch_text = "כניסה עם שם משתמש"
    switch_short = "שם משתמש"
    selector_templates = [
        "button:has-text('{text}')",
        "a:has-text('{text}')",
        "[role='button']:has-text('{text}')",
        "[role='tab']:has-text('{text}')",
        "[aria-label*='{text}']",
        "[title*='{text}']",
        "text={text}",
    ]
    for _ in range(8):
        for target in _login_targets(page):
            locators = [
                target.get_by_role("tab", name=switch_text, exact=False),
                target.get_by_role("button", name=switch_text, exact=False),
                target.get_by_role("link", name=switch_text, exact=False),
                target.get_by_text(switch_text, exact=False),
                target.get_by_text(switch_short, exact=False),
            ]
            for text in (switch_text, switch_short):
                for template in selector_templates:
                    locators.append(target.locator(template.format(text=text)))

            for locator in locators:
                if _click_first_clickable(locator):
                    page.wait_for_timeout(900)
                    return True

            if _switch_via_js(target, switch_short):
                page.wait_for_timeout(900)
                return True
        page.wait_for_timeout(800)
    return False


def _prefill_credentials(page, username: str, password: str) -> tuple[bool, bool]:
    username_selectors = [
        "input[autocomplete='username']",
        "input[name='username']",
        "input[name='userName']",
        "input[name='userCode']",
        "input[name='userid']",
        "input[name='id']",
        "input[name*='user' i]",
        "input[name*='login' i]",
        "input[name*='id' i]",
        "input[id*='user' i]",
        "input[id*='login' i]",
        "input[id*='id' i]",
        "input[aria-label*='שם משתמש']",
        "input[placeholder*='שם משתמש']",
        "input[aria-label*='תעודת']",
        "input[placeholder*='תעודת']",
        "input[type='tel']",
        "input[type='number']",
        "input[type='email']",
        "input[type='text']",
    ]
    password_selectors = [
        "input[autocomplete='current-password']",
        "input[name='password']",
        "input[id*='pass' i]",
        "input[type='password']",
    ]
    username_filled = False
    password_filled = False
    for target in _login_targets(page):
        if not username_filled:
            username_filled = _try_fill_first(target, username_selectors, username)
            if not username_filled:
                username_filled = _fill_via_js_fallback(target, username, is_password=False)
        if not password_filled:
            password_filled = _try_fill_first(target, password_selectors, password)
            if not password_filled:
                password_filled = _fill_via_js_fallback(target, password, is_password=True)
        if username_filled and password_filled:
            break

    return username_filled, password_filled


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Capture CAL HAR from an interactive browser session")
    parser.add_argument(
        "--target-url",
        default=TARGET_URL,
        help="Start page URL for browser session",
    )
    parser.add_argument(
        "--login-prompt",
        default=LOGIN_PROMPT,
        help="Text shown before waiting for Enter",
    )
    parser.add_argument(
        "--dotenv-path",
        default=DEFAULT_DOTENV_PATH,
        help="Path to .env file with credentials",
    )
    parser.add_argument(
        "--username-env",
        default=DEFAULT_USERNAME_ENV,
        help="Environment variable name for username",
    )
    parser.add_argument(
        "--password-env",
        default=DEFAULT_PASSWORD_ENV,
        help="Environment variable name for password",
    )
    args = parser.parse_args(argv)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dotenv_values: dict[str, str] = {}
    dotenv_source: Path | None = None
    for candidate in _dotenv_candidates(args.dotenv_path):
        if candidate.exists():
            dotenv_values = _load_dotenv(candidate)
            dotenv_source = candidate
            break

    username_names = [args.username_env, *FALLBACK_USERNAME_ENVS]
    password_names = [args.password_env, *FALLBACK_PASSWORD_ENVS]
    username, username_source, username_var = _read_credential(username_names, dotenv_values)
    password, password_source, password_var = _read_credential(password_names, dotenv_values)

    print("Открываю Chromium и начинаю запись HAR...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            record_har_path=str(HAR_PATH),
            record_har_content="embed",
            record_har_mode="full",
        )

        page = context.new_page()
        page.goto(args.target_url, wait_until="domcontentloaded")
        page.wait_for_timeout(1200)

        if username and password:
            print(
                "Учетные данные найдены. "
                f"login={username_var}({username_source}), password={password_var}({password_source})."
            )
            switched = _switch_to_username_login(page)
            if switched:
                print("Переключил форму входа на «כניסה עם שם משתמש».")
            else:
                print("Автопереключение на «כניסה עם שם משתמש» не сработало.")
                print("Переключите форму вручную и нажмите Enter, после этого повторю автозаполнение.")
                input()
            username_filled, password_filled = _prefill_credentials(page, username, password)
            if username_filled and password_filled:
                print("Логин и пароль подставлены из переменных окружения.")
            elif username_filled and not password_filled:
                print("Логин подставлен, пароль нет. Введите пароль вручную.")
            elif password_filled and not username_filled:
                print("Пароль подставлен, логин нет. Введите логин вручную.")
            else:
                print("Переменные найдены, но поля входа не обнаружены автоматически. Заполните вручную.")
        else:
            dotenv_info = f" Файл переменных: {dotenv_source}." if dotenv_source else ""
            print(
                "Переменные окружения с логином/паролем не найдены. "
                f"Ожидаются {args.username_env}/{args.password_env} "
                f"(или {FALLBACK_USERNAME_ENVS[0]}/{FALLBACK_PASSWORD_ENVS[0]})."
                f"{dotenv_info}"
            )

        print(args.login_prompt)
        input()

        context.storage_state(path=str(STORAGE_STATE_PATH))
        context.close()
        browser.close()

    print(f"HAR сохранён: {HAR_PATH}")
    print(f"Storage state сохранён: {STORAGE_STATE_PATH}")


if __name__ == "__main__":
    main()
