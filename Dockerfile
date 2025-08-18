FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY pyproject.toml .
COPY uv.lock .

ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
WORKDIR /code/
RUN uv sync --all-groups --frozen

COPY tests/ tests
COPY flake8.cfg .
COPY src/ src

CMD ["python", "-u", "/code/src/component.py"]