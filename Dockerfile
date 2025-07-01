FROM apache/airflow:2.7.3-python3.10

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        openssl \
        build-essential \
        gcc \
        libssl-dev \
        libffi-dev \
        && update-ca-certificates \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt
