from tools.research.redaction import redact_headers, redact_json


def test_redact_headers() -> None:
    headers = {"Authorization": "abc", "X-Test": "ok"}
    out = redact_headers(headers, ["authorization", "token"])
    assert out["Authorization"] == "<redacted>"
    assert out["X-Test"] == "ok"


def test_redact_json_nested() -> None:
    payload = {"token": "abc", "user": {"password": "secret", "name": "n"}}
    out = redact_json(payload, ["token", "password"])
    assert out["token"] == "<redacted>"
    assert out["user"]["password"] == "<redacted>"
    assert out["user"]["name"] == "n"
