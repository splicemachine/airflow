FROM apache/airflow:2.0.1
USER root

ARG sm_version=3.1.0.2002

ENV SRC_HOME=/opt/airflow
ENV SPARK_CONF_DIR=/etc/spark/conf
ENV JAVA_HOME="/opt/jdk"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY build/spark_config/spark.log4j.properties ${SPARK_CONF_DIR}/
COPY build/spark_config/spark-defaults.conf ${SPARK_CONF_DIR}

RUN \
    mkdir -p $SRC_HOME/eNSDS            && \
    mkdir -p $SRC_HOME/eNSDS/jars       && \
    curl -kLs http://repository.splicemachine.com/nexus/content/groups/public/com/splicemachine/db-client/$sm_version/db-client-${sm_version}.jar           -o $SRC_HOME/eNSDS/jars/db-client.jar && \
    curl -kLs https://jpanko-splice.s3.amazonaws.com/eNSDS-Spark3.0.1/splice_spark2-3.1.0.2002-shaded.jar                                                   -o $SRC_HOME/eNSDS/splice_spark2.jar && \
    curl -kLs https://repository.cloudera.com/artifactory/cloudera-repos/org/apache/kafka/kafka-clients/2.2.1-cdh6.3.0/kafka-clients-2.2.1-cdh6.3.0.jar     -o $SRC_HOME/eNSDS/jars/kafka-clients.jar

COPY build/requirements.txt /tmp/requirements.txt
# COPY build/config/__init__.py /usr/local/airflow/config/__init__.py
# COPY build/config/log_config.py /usr/local/airflow/config/log_config.py

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
    # wget \
    # zlib1g-dev

RUN \
    curl --insecure -o jdk-8u121-linux-x64.tar.gz https://s3.amazonaws.com/splice-jdks/jdk-8u121-linux-x64.tar.gz && \
    tar -C /opt -xzf jdk-8u121-linux-x64.tar.gz && \
    mv /opt/jdk1.8.0_121 ${JAVA_HOME} && \
    rm jdk-8u121-linux-x64.tar.gz && \
    chmod +x $JAVA_HOME/bin/*

# RUN cd /tmp && \
#   wget https://www.python.org/ftp/python/3.7.3/Python-3.7.3.tgz && \
#   tar xzf Python-3.7.3.tgz && \
#   cd Python-3.7.3  && \
#   ./configure --enable-shared --enable-optimizations && \
#   make altinstall && \
#   ln -sfn /usr/local/bin/python3.7 /usr/bin/python3 && \
#   ln -sfn /usr/local/bin/pip3.7 /usr/bin/pip3 && \
#   rm -rf /tmp/Python-3.7.3 && \
#   rm /tmp/rm -rf Python-3.7.3.tgz && \
#   ldconfig /usr/local/lib && \
#   ln -s /usr/share/pyshared/lsb_release.py /usr/local/lib/python3.7/site-packages/lsb_release.py

USER airflow

EXPOSE 9876
#Python Package Dependencies for Airflow
RUN pip install --user -r /tmp/requirements.txt
