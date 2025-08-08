"""
Deprecated helper. The canonical flow now runs via:

    - scripts/external_matching/ultimate_spotify_matcher.py
    - scripts/orchestrate.py --kaggle

This shim forwards to the current script to avoid breaking bookmarks.
"""

import runpy
from pathlib import Path

target = Path(__file__).parent / "scripts" / "external_matching" / "ultimate_spotify_matcher.py"
runpy.run_path(str(target), run_name="__main__")
