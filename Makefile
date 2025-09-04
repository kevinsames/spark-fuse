.PHONY: lint test format pre-commit install

install:
	pip install -e .[dev]

lint:
	ruff check src tests

format:
	ruff format src tests

test:
	pytest -q

pre-commit:
	pre-commit run --all-files
