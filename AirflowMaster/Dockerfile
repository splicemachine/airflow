FROM python:3.7-slim
USER root

ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

#To prevent Kubernetes API from timing out while running the airflow scheduler
ENV AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__KUBE_CLIENT_REQUEST_TIMEOUT_SEC=50
ENV AIRFLOW__ELASTICSEARCH__LOG_ID_TEMPLATE={{dag_id}}-{{task_id}}-{{execution_date}}-{{try_number}}

RUN mkdir -p ${AIRFLOW_USER_HOME}/scripts
COPY scripts/entrypoint.sh ${AIRFLOW_USER_HOME}/scripts/entrypoint.sh
#DAGS
#COPY build/config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
COPY build/config/__init__.py /usr/local/airflow/config/__init__.py
COPY build/config/log_config.py /usr/local/airflow/config/log_config.py
COPY build/requirements.txt /tmp/requirements.txt

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
    supervisor

RUN echo "deb http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg |  apt-key add -
RUN apt-get update && apt-get install -y google-cloud-sdk

RUN mkdir -p ${AIRFLOW_HOME} && \
    chmod +x ${AIRFLOW_HOME} && \
    cd ${AIRFLOW_HOME} && \
    useradd -ms /bin/bash airflow && \
    usermod -a -G sudo airflow && \
    chmod 666 -R ${AIRFLOW_HOME}


EXPOSE 9876
#Python Package Dependencies for Airflow
RUN pip install -r /tmp/requirements.txt

#Remote Logging Setup
RUN cd ${AIRFLOW_HOME} && \
    mkdir -p ${AIRFLOW_HOME}/logs && \
    mkdir -p ${AIRFLOW_HOME}/config && \
    mkdir -p ${AIRFLOW_HOME}/logs/scheduler && \
    mkdir -p ${AIRFLOW_HOME}/dags && \
    chmod +x -R ${AIRFLOW_HOME}/logs && \
    chmod +x -R ${AIRFLOW_HOME}/config && \
    chmod +x -R ${AIRFLOW_HOME}/dags


#Python Packages for Azure
#RUN pip install azure-mgmt-compute azure-mgmt-storage azure-mgmt-resource azure-keyvault-secrets azure-storage-blob
#RUN pip install azure-storage-file-datalake --pre mysql-connector-python-rf


#Supervisord
COPY build/config/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
RUN chmod +x /etc/supervisor/conf.d/supervisord.conf && \
    chmod +x /usr/local/airflow/scripts/entrypoint.sh
ENV AIRFLOW__KUBERNETES__AIRFLOW_CONFIGMAP=airflow-configmap


ENTRYPOINT "/usr/local/airflow/scripts/entrypoint.sh"
#CMD ["/usr/bin/supervisord"]
