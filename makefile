# Variables
VENV_NAME := .venv
PYTHON := python3
PIP := $(VENV_NAME)/bin/pip
PY := $(VENV_NAME)/bin/python

# target by default
.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make venv        Create virtual environment"
	@echo "  make install     Install dependencies"
	@echo "  make run         Run ETL job"
	@echo "  make clean       Remove virtual environment and outputs"

# Create virtualenv
.PHONY: venv
venv:
	$(PYTHON) -m venv $(VENV_NAME)
	@echo "Virtual environment created"

# Install dependencies
.PHONY: install
install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "Dependencies installed"

# Execute tests
.PHONY: test
test:
	PYTHONPATH=. $(PY) -m pytest tests/

# Execute etl
.PHONY: run
run:
	PYTHONPATH=. $(PY) -m src.etl_job

.PHONY: all
all: install test run
	@echo "------------------------------------------------"
	@echo "Full pipeline executed successfully!"
	@echo "1. Environment prepared."
	@echo "2. Tests passed."
	@echo "3. ETL completed & Sanity Checks verified."
	@echo "------------------------------------------------"

# clean
# 	rm -rf output/*
.PHONY: clean
clean:
	rm -rf $(VENV_NAME)
	rm -rf output/*
	rm -rf .idea
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	@echo "Project cleaned (virtualenv, outputs and cache removed)"