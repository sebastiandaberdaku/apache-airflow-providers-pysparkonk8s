ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION=3.10
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root
# INSTALL openjdk
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
# INSTALL other dependencies
WORKDIR /home/airflow
COPY ./requirements.txt .

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
RUN pip install \
    --no-cache-dir \
    --ignore-installed \
    --trusted-host pypi.python.org \
    --requirement requirements.txt  \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"