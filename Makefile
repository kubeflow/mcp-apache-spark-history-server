.PHONY: ensure-uv sync install uninstall test

ensure-uv:
	@command -v uv >/dev/null 2>&1 || { echo "uv is not installed. Install from https://docs.astral.sh/uv/"; exit 1; }

sync: ensure-uv
	uv sync

install: ensure-uv
	uv tool install . --force

uninstall: ensure-uv
	uv tool uninstall mcp-apache-spark-history-server

test: sync
	uv run pytest tests/ -x -q
