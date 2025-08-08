"""
Deprecated: Use scripts/external_matching/ultimate_spotify_matcher.py

This top-level helper is retained to avoid breaking links. It simply forwards
to the canonical script under scripts/.
"""

import runpy
from pathlib import Path

CANONICAL = Path(__file__).parent / "scripts" / "external_matching" / "kaggle_metadata_match.py"
ALT = Path(__file__).parent / "scripts" / "external_matching" / "ultimate_spotify_matcher.py"

target = CANONICAL if CANONICAL.exists() else ALT
runpy.run_path(str(target), run_name="__main__")
