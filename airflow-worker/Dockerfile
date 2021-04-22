FROM apache/airflow:2.0.1
USER root

ARG sm_version=3.1.0.2002

ENV SRC_HOME=/opt/airflow
ENV SPARK_CONF_DIR=/etc/spark/conf
ENV JAVA_HOME="/opt/jdk"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY airflow-worker/build/spark_config/* ${SPARK_CONF_DIR}/

RUN \
    mkdir -p $SRC_HOME/eNSDS            && \
    mkdir -p $SRC_HOME/eNSDS/jars       && \
    curl -kLs http://repository.splicemachine.com/nexus/content/groups/public/com/splicemachine/db-client/$sm_version/db-client-${sm_version}.jar           -o $SRC_HOME/eNSDS/jars/db-client.jar && \
    curl -kLs https://jpanko-splice.s3.amazonaws.com/eNSDS-Spark3.0.1/splice_spark2-3.1.0.2002-shaded.jar                                                   -o $SRC_HOME/eNSDS/splice_spark2.jar && \
    curl -kLs https://repository.cloudera.com/artifactory/cloudera-repos/org/apache/kafka/kafka-clients/2.2.1-cdh6.3.0/kafka-clients-2.2.1-cdh6.3.0.jar     -o $SRC_HOME/eNSDS/jars/kafka-clients.jar

RUN apt-get update && apt-get install --yes \
    sudo \
    git \
    vim \
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

USER airflow

EXPOSE 9876
#Python Package Dependencies for Airflow
RUN pip install --user -r /tmp/requirements.txt

COPY dags/* ${AIRFLOW_HOME}/dags/

ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"
