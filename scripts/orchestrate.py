#!/usr/bin/env python3
import argparse
import subprocess
import sys
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parents[1]
VENV_PY = ROOT / ".venv" / "bin" / "python"
PY = str(VENV_PY if VENV_PY.exists() else sys.executable)

STEPS = {
    "combine": [PY, str(ROOT / "scripts" / "data_processing" / "combine-history.py")],
    "clean": [PY, str(ROOT / "scripts" / "data_processing" / "clean-history.py")],
    "kaggle": [PY, str(ROOT / "scripts" / "external_matching" / "ultimate_spotify_matcher.py")],
    "api": [PY, str(ROOT / "scripts" / "spotify_api" / "smart_metadata_enrichment.py")],
    "merge": [PY, str(ROOT / "scripts" / "enrichment" / "merge_enrichments.py")],
    "coverage": [PY, str(ROOT / "scripts" / "analysis" / "coverage_analysis.py")],
}

ORDER = ["combine", "clean", "kaggle", "api", "merge", "coverage"]


def run_step(name: str, cmd: list[str]) -> int:
    print(f"\n=== ▶ {name.upper()} ===")
    print("$", " ".join(cmd))
    start = time.time()
    try:
        proc = subprocess.run(cmd, cwd=ROOT, check=False)
        code = proc.returncode
    except KeyboardInterrupt:
        print("Interrupted by user.")
        return 130
    dur = time.time() - start
    print(f"=== ✓ {name} finished in {dur:.1f}s (code={code}) ===\n")
    return code


def main():
    parser = argparse.ArgumentParser(description="Run the Spotify data pipeline with simple flags.")
    parser.add_argument("--all", action="store_true", help="Run all steps in order")
    parser.add_argument("--combine", action="store_true")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--kaggle", action="store_true", help="Enrich using external Kaggle dataset")
    parser.add_argument("--api", action="store_true", help="Enrich using Spotify API (smart skipping)")
    parser.add_argument("--merge", action="store_true", help="Merge all enrichments into a final dataset")
    parser.add_argument("--coverage", action="store_true", help="Print current coverage analysis")
    args = parser.parse_args()

    # Default to --all when no flags are provided
    selected = any([args.combine, args.clean, args.kaggle, args.api, args.merge, args.coverage])
    steps = ORDER if (args.all or not selected) else [
        name for name in ORDER
        if getattr(args, name)
    ]

    # Ensure output dirs exist (idempotent)
    (ROOT / "data" / "processed").mkdir(parents=True, exist_ok=True)
    (ROOT / "data" / "enriched").mkdir(parents=True, exist_ok=True)

    exit_code = 0
    for name in steps:
        exit_code = run_step(name, STEPS[name])
        if exit_code not in (0, None):
            print(f"Step '{name}' failed with code {exit_code}. Stopping.")
            break
    sys.exit(exit_code or 0)


if __name__ == "__main__":
    main()
