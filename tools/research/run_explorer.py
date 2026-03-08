from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.research.config import load_configs
from tools.research.runtime import run_target_exploration


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unified bank research explorer")
    parser.add_argument("--target", required=True, help="Target config name, e.g. discount or cal_digital")
    parser.add_argument(
        "--mechanism-config",
        default="configs/research/mechanism.yaml",
        help="Path to shared mechanism config",
    )
    parser.add_argument(
        "--targets-dir",
        default="configs/research/targets",
        help="Directory with target YAML files",
    )
    parser.add_argument("--headful", action="store_true", help="Force headful mode")
    parser.add_argument(
        "--keep-open",
        action="store_true",
        help="Keep browser open until Ctrl+C (overrides --keep-open-seconds)",
    )
    parser.add_argument(
        "--keep-open-seconds",
        type=int,
        default=30,
        help="How long to keep browser open after login stage",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()

    mechanism_path = Path(args.mechanism_config)
    target_path = Path(args.targets_dir) / f"{args.target}.yaml"

    mechanism, target = load_configs(mechanism_path, target_path)
    if args.headful:
        mechanism.browser["headless"] = False

    return run_target_exploration(
        mechanism,
        target,
        headful=args.headful,
        keep_open=args.keep_open,
        keep_open_seconds=args.keep_open_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
