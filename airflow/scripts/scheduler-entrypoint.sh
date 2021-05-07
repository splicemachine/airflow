#!/usr/bin/env bash

cp /mnt/airflow-conf/airflow.cfg /opt/airflow/airflow.cfg
until airflow db check; do sleep 5; done
airflow db upgrade
airflow scheduler
