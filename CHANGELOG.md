# Changelog

All notable changes to fusetools are documented here.

## [1.0.0] — 2026-03-21

### Changed
- Replaced `setup.py` / `requirements.txt` with `pyproject.toml` (hatchling)
- Pruned 21 unused modules down to 5 core modules: `cloud_tools`, `db_tools`, `gsuite_tools`, `transfer_tools`, `logging_tools`
- Merged `db_conn_tools` + `db_etl_tools` into single `db_tools` module
- All optional dependencies are now lazy-imported inside methods
- `pandas` is the only required core dependency
- Optional deps organized into extras: `aws`, `firebase`, `gcp`, `gsuite`, `db`, `transfer`, `all`
- Removed `colorama` — plain print replaces `Fore.RED`
- Removed `selenium` / `Web` class from `transfer_tools`
- Added type hints and `# MARK: -` section comments throughout
- Added `py.typed` marker for PEP 561 compliance

### Added
- `pyproject.toml` with hatchling build backend and optional dependency extras
- `uv.lock` for reproducible installs
- `Makefile` with `test`, `lint`, `format` targets
- `.pre-commit-config.yaml` (ruff, mypy)
- GitHub Actions CI (lint + unit tests on push/PR to master)
- GitHub Actions release workflow (build → PyPI publish → GitHub release on `v*` tags)
- Unit tests (13 tests across all 5 modules)
- Integration test stubs
- `.env.example` for integration test configuration
