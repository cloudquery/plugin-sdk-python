test:
	uv run pytest .

fmt:
	uv run black .

fmt-check:
	uv run black --check .
