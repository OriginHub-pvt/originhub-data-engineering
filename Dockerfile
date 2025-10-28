FROM apache/airflow:3.1.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

USER airflow

