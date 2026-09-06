# Convenience targets. Everything runs through uv so the lockfile is authoritative.
CONFIG ?= configs/local.yaml
RUN_DATE ?=
DATE_FLAG := $(if $(RUN_DATE),--run-date $(RUN_DATE),)

.PHONY: sync generate train run inspect all test test-e2e lint fmt clean

sync:            ## install/refresh the virtualenv from uv.lock
	uv sync

generate:        ## write synthetic raw partitions into storage.root
	uv run fraudflow generate -c $(CONFIG) $(DATE_FLAG)

train:           ## train + promote a model to models/current
	uv run fraudflow train -c $(CONFIG) $(DATE_FLAG)

run:             ## score one day end-to-end
	uv run fraudflow run -c $(CONFIG) $(DATE_FLAG)

inspect:         ## show top blocked transactions for the date
	uv run fraudflow inspect -c $(CONFIG) $(DATE_FLAG)

all: generate train run inspect

test:            ## fast unit tests (no Ray)
	uv run pytest -q -m "not e2e"

test-e2e:        ## full pipeline on a tiny lake in a temp dir (~1-2 min)
	uv run pytest -q -m e2e

lint:
	uv run ruff check src tests && uv run ruff format --check src tests

fmt:
	uv run ruff format src tests && uv run ruff check --fix src tests

clean:
	rm -rf data/curated data/runs data/models
