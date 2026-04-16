FROM python:trixie

# Set working directory
ADD ./pyproject.toml /app/
WORKDIR /app

# Install uv package manager
COPY --from=ghcr.io/astral-sh/uv:0.6.3 /uv /uvx /bin/

# Initialize virtual environment
RUN uv venv

# Open a bash shell (with the uv environment)
CMD ["bash"]