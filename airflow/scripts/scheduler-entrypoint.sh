#!/usr/bin/env bash

cp /mnt/airflow-conf/airflow.cfg /opt/airflow/airflow.cfg
until airflow db check; do sleep 5; done
airflow db upgrade
if [[ -z "${SPARK_PORT}" ]]; then
  airflow connections add 'splice_spark' \
    --conn-type 'spark' \
    --conn-host $SPARK_HOST
else
  airflow connections add 'splice_spark' \
    --conn-type 'spark' \
    --conn-host $SPARK_HOST \
    --conn-port $SPARK_PORT
fi
airflow scheduler
