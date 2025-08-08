# Convenience Makefile
PY?=$(shell [ -x .venv/bin/python ] && echo .venv/bin/python || which python3)

.PHONY: all setup combine clean kaggle api merge coverage orchestrate

all: orchestrate

setup:
	@echo "Creating data directories..."
	@mkdir -p data/processed data/enriched
	@echo "Install deps into venv (optional): pip install -r requirements.txt"

combine:
	$(PY) scripts/data_processing/combine-history.py

clean:
	$(PY) scripts/data_processing/clean-history.py

kaggle:
	$(PY) scripts/external_matching/ultimate_spotify_matcher.py

api:
	$(PY) scripts/spotify_api/smart_metadata_enrichment.py

merge:
	$(PY) scripts/enrichment/merge_enrichments.py

coverage:
	$(PY) scripts/analysis/coverage_analysis.py

orchestrate:
	$(PY) scripts/orchestrate.py --all
