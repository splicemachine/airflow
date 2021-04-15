#!/usr/bin/env bash

cp /mnt/airflow-conf/airflow.cfg /opt/airflow/airflow.cfg
airflow users create --username splice --lastname machine --firstname splice --email cloud@splicemachine.com --role Admin --password $SPLICE_ADMIN_PWD || true
airflow webserver
