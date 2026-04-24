# Release Checklist

Use this checklist for the first stable public release and for future tagged releases.

## One-Time Setup

- Configure PyPI Trusted Publishing for `biprakanta/escrowmint-py`
- Confirm GitHub Actions is allowed to create release PRs, tags, and releases
- Confirm the `badges` branch can be updated by CI

## Pre-Release Validation

- Ensure `main` is green in CI
- Run a local smoke test:
  - `uv sync --dev`
  - `uv run ruff check`
  - `uv run pytest`
  - `uv build`
- Confirm README install and quickstart examples still match the shipped API
- Confirm `pyproject.toml` metadata is correct

## Release Execution

- Merge the current Release Please PR
- Confirm the `vX.Y.Z` tag is created
- Confirm the GitHub release is created with generated notes
- Confirm the tag-triggered publish workflow succeeds
- Confirm the package appears on PyPI

## Post-Release Smoke Test

- Create a clean virtual environment
- Run `pip install escrowmint==X.Y.Z`
- Import the package and execute a minimal `Client.from_url(...)` flow against Redis
- Confirm the README badge row and release links reflect the new release
