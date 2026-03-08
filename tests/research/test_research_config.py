from pathlib import Path

from tools.research.config import load_configs


def test_load_configs_discount() -> None:
    mechanism, target = load_configs(
        Path("configs/research/mechanism.yaml"),
        Path("configs/research/targets/discount.yaml"),
    )

    assert mechanism.browser["persistent_profile"] is True
    assert target.provider_name == "discount"
    assert "DISCOUNT_USERNAME" in target.env["required"]
