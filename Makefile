.PHONY: install demo test test-no-llm lint typecheck clean help

PYTHON ?= python3
VENV   ?= .venv
PIP    := $(VENV)/bin/pip
PY     := $(VENV)/bin/python
PYTEST := $(VENV)/bin/pytest

help:
	@echo "Onda v0.1 — Makefile targets"
	@echo "  make install      create venv and install package + dev deps"
	@echo "  make demo         run the two-node demo from examples/"
	@echo "  make test         run the full pytest suite"
	@echo "  make test-no-llm  run unit tests only (skip Ollama-dependent ones)"
	@echo "  make lint         ruff lint"
	@echo "  make typecheck    mypy on src/"
	@echo "  make clean        remove venv and caches"

$(VENV)/bin/python:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip wheel

install: $(VENV)/bin/python
	$(PIP) install -e ".[dev]"
	@echo ""
	@echo "Installed. Activate with: source $(VENV)/bin/activate"
	@echo "Then run: onda --help"

demo:
	@bash examples/demo_two_nodes.sh

test:
	$(PYTEST) -v

test-no-llm:
	ONDA_LLM_BACKEND=echo $(PYTEST) -v -m "not requires_ollama"

lint:
	$(VENV)/bin/ruff check src tests

typecheck:
	$(VENV)/bin/mypy src

clean:
	rm -rf $(VENV) .pytest_cache .ruff_cache .mypy_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
