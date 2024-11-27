FROM apache/airflow:latest-python3.12

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements
COPY requirements.txt /opt/airflow/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy your project files
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/