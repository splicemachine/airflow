#!/usr/bin/env bash

cp /mnt/airflow-conf/airflow.cfg /usr/local/airflow/airflow.cfg
airflow initdb
airflow create_user --username splice --lastname machine --firstname splice --email cloud@splicemachine.com --role Admin --password $SPLICE_ADMIN_PWD || true
/usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
