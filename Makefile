.PHONY: format lint test integration_tests

test:
	uv run pytest --disable-socket --allow-unix-socket tests/unit_tests/

integration_tests:
	uv run pytest tests/integration_tests/ -v

lint:
	uv run ruff check .
	uv run ruff format . --diff
	uv run mypy fusetools

format:
	uv run ruff format .
	uv run ruff check --select I --fix .
