FROM python:trixie

# Install uv package manager
COPY --from=ghcr.io/astral-sh/uv:0.6.3 /uv /uvx /bin/
ENV UV_PROJECT_ENVIRONMENT=/venv
RUN mkdir -p /venv
ENV PATH="/venv/bin:${PATH}"

# Set working directory
WORKDIR /app

# Initialize virtual environment
COPY ./pyproject.toml /app/
RUN uv venv && uv sync