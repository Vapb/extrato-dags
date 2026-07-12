export DAGSTER_HOME := $(shell pwd)/.dagster

dev:
	mkdir -p $(DAGSTER_HOME)
	uv run dagster dev

test:
	uv run pytest tests/
