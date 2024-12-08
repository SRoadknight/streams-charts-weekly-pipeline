FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install poetry
RUN pip install poetry

WORKDIR /app

# Copy dependency files only
COPY pyproject.toml poetry.lock ./

# Install dependencies using poetry
# Disable virtualenv creation as we're in a container
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# Create necessary directories
RUN mkdir -p .file_versions && \
    chmod -R 777 .file_versions

# Copy the rest of the application
COPY . .

EXPOSE 6789

CMD ["mage", "start", "."]