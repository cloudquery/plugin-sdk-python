FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency files and install
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy the code and install the project
COPY cloudquery cloudquery
COPY main.py .
RUN uv sync --frozen --no-dev

EXPOSE 7777

ENTRYPOINT ["uv", "run", "python3", "main.py"]

CMD ["serve", "--address", "[::]:7777", "--log-format", "json", "--log-level", "info"]
