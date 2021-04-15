#!/usr/bin/env bash

cp /mnt/airflow-conf/airflow.cfg /opt/airflow/airflow.cfg
airflow db upgrade
airflow scheduler
