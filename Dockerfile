FROM apache/airflow:2.7.3

USER root

# Install certs AND openssl (fixes missing/broken SSL chain support)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        openssl && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
