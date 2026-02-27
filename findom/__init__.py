from __future__ import annotations

from pathlib import Path

# Keep legacy top-level packages (cal/, utils/) available as findom.cal/findom.utils.
_PKG_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _PKG_DIR.parent
__path__ = [str(_PKG_DIR), str(_PROJECT_ROOT)]

__all__ = ["__path__"]
