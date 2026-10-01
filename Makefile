.PHONY: sync test lint fmt skeleton check clean

sync:
	uv sync --extra dev

test:
	uv run python -m pytest -q

lint:
	uv run ruff check src tests tools

fmt:
	uv run ruff format src tests tools

skeleton:
	uv run python tools/gen_skeleton.py

check:
	uv run python tools/gen_skeleton.py --check

# 单进程启动入口（grouppig.main）由 t10 集成任务提供

clean:
	rm -rf .pytest_cache .ruff_cache var
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
