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
    mkdir -p /var/log/supervisor && \
    chmod +x -R ${AIRFLOW_HOME}/logs && \
    chmod +x -R ${AIRFLOW_HOME}/config && \
    chmod +x -R ${AIRFLOW_HOME}/logs/scheduler && \
    chmod +x -R ${AIRFLOW_HOME}/dags

COPY config/* $SRC_HOME/config/
COPY airflow/build/config/webserver_config.py $SRC_HOME/webserver_config.py
COPY airflow/build/requirements.txt /tmp/requirements.txt

#Python Package Dependencies for Airflow
RUN pip install --user -r /tmp/requirements.txt

COPY dags/* ${AIRFLOW_HOME}/dags/

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow"