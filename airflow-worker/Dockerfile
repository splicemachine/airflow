FROM apache/airflow:2.1.0-python3.7
USER root

ARG sm_version=3.1.0.2002

ENV SRC_HOME=/opt/airflow
ENV SPARK_CONF_DIR=/etc/spark/conf
ENV JAVA_HOME="/opt/jdk"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY airflow-worker/build/spark_config/* ${SPARK_CONF_DIR}/

RUN apt-get update && apt-get install --yes \
    sudo \
    git \
    gcc \
    g++ \
    build-essential \
    libssl-dev \
    libffi-dev \
    unixodbc \
    unixodbc-dev \
    iodbc

RUN \
    curl --insecure -o jdk-8u121-linux-x64.tar.gz https://s3.amazonaws.com/splice-jdks/jdk-8u121-linux-x64.tar.gz && \
    tar -C /opt -xzf jdk-8u121-linux-x64.tar.gz && \
    mv /opt/jdk1.8.0_121 ${JAVA_HOME} && \
    rm jdk-8u121-linux-x64.tar.gz && \
    chmod +x $JAVA_HOME/bin/*

COPY airflow-worker/build/requirements.txt /tmp/requirements.txt
COPY config/* ${AIRFLOW_HOME}/config/

RUN mkdir -p ${AIRFLOW_HOME}/spark_apps && chmod +x -R ${AIRFLOW_HOME}/spark_apps

USER airflow

EXPOSE 9876

#Python Package Dependencies for Airflow
RUN pip install --upgrade pip setuptools && pip install --user -r /tmp/requirements.txt && pip cache purge
COPY dags/* ${AIRFLOW_HOME}/dags/
COPY spark_apps/* ${AIRFLOW_HOME}/spark_apps/

ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"
