PY ?= python3
export PYTHONPATH := src

.PHONY: check lint test smoke plan run serve sample clean

check: lint test smoke      ## everything CI runs

lint:
	ruff check src tests tools
	ruff format --check src tests tools

test:
	$(PY) -m pytest -q

smoke:                      ## ingest the real sample without a broker, then the retention table
	rm -f /tmp/collector-smoke.db*
	$(PY) -m collector --db /tmp/collector-smoke.db replay data/sample_incubator.jsonl
	$(PY) -m collector --db /tmp/collector-smoke.db compact
	$(PY) -m collector plan

run:                        ## collector + API against the broker in .env
	$(PY) -m collector run

serve:                      ## API only
	$(PY) -m collector serve

sample:                     ## regenerate data/sample_incubator.jsonl from the sibling repo
	$(PY) tools/make_sample.py ../incubator-telemetry-analysis/data/hive-1C5A00_history_2026-10-03.json data/sample_incubator.jsonl

clean:
	rm -rf .pytest_cache .ruff_cache src/*.egg-info /tmp/collector-smoke.db*
	find . -name __pycache__ -type d -exec rm -rf {} +
