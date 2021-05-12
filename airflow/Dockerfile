FROM apache/airflow:2.0.1
USER root

ENV SRC_HOME=/opt/airflow

RUN mkdir -p ${AIRFLOW_HOME}/scripts
COPY airflow/scripts/* ${AIRFLOW_HOME}/scripts/

RUN echo "deb http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg |  apt-key add -

RUN apt-get update && apt-get install --yes \
    sudo \
    git \
    vim \
    cron \
    gcc \
    g++ \
    unzip \
    unixodbc-dev \
    gnupg \
    curl \
    supervisor \
    google-cloud-sdk

#Python Packages for Azure
#RUN pip install azure-mgmt-compute azure-mgmt-storage azure-mgmt-resource azure-keyvault-secrets azure-storage-blob
#RUN pip install azure-storage-file-datalake --pre mysql-connector-python-rf

RUN chmod a+x -R /opt/airflow/scripts

USER airflow

EXPOSE 9876

#Remote Logging Setup
RUN cd ${AIRFLOW_HOME} && \
    mkdir -p ${AIRFLOW_HOME}/logs && \
    mkdir -p ${AIRFLOW_HOME}/config && \
    mkdir -p ${AIRFLOW_HOME}/logs/scheduler && \
    mkdir -p ${AIRFLOW_HOME}/dags && \
    mkdir -p ${AIRFLOW_HOME}/spark_apps && \
    mkdir -p /var/log/supervisor && \
    chmod +x -R ${AIRFLOW_HOME}/logs && \
    chmod +x -R ${AIRFLOW_HOME}/config && \
    chmod +x -R ${AIRFLOW_HOME}/logs/scheduler && \
    chmod +x -R ${AIRFLOW_HOME}/dags && \
    chmod +x -R ${AIRFLOW_HOME}/spark_apps

COPY config/* $SRC_HOME/config/
COPY airflow/build/config/webserver_config.py $SRC_HOME/webserver_config.py
COPY airflow/build/requirements.txt /tmp/requirements.txt

#Python Package Dependencies for Airflow
RUN pip install --user -r /tmp/requirements.txt

COPY dags/* ${AIRFLOW_HOME}/dags/
COPY spark_apps/* ${AIRFLOW_HOME}/spark_apps/

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow"

ENV AIRFLOW__KUBERNETES__WORKER_CONTAINER_TAG=0.0.5_mn018
ENV AIRFLOW__KUBERNETES__DELETE_WORKER_PODS=False
ENV SPARK_HOST="k8s://kubernetes.default.svc.cluster.local"
ENV SPARK_PORT=443
# ENV AIRFLOW__KUBERNETES__CLUSTER_CONTEXT=test
ENV SPARK_DOCKER_IMAGE="splicemachine/sm_k8_spark-3.0.0:latest"
ENV SPLICE_REL_NAME="splicedb"
ENV SPLICE_NAMESPACE="test"
ENV TZ="UTC"
