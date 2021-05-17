from datetime import date, timedelta, datetime
from os import environ as env_vars, path
import json
import re
from dateutil.relativedelta import relativedelta
from splicemachine.features.pipelines import AggWindow

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.decorators import dag
from config.spark_config import spark_defaults

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

def agg_window_to_timedelta(interval: str):
    try:
        length, window = re.search(r'(\d+)([a-z]{1,2})', interval).groups()
        length = int(length)
    except:
        print(f'Invalid schedule interval: {interval}')
        return None

    if window == AggWindow.SECOND:
        return relativedelta(seconds=length)
    elif window == AggWindow.MINUTE:
        return relativedelta(minutes=length)
    elif window == AggWindow.HOUR:
        return relativedelta(hours=length)
    elif window == AggWindow.DAY:
        return relativedelta(days=length)
    elif window == AggWindow.WEEK:
        return relativedelta(weeks=length)
    elif window == AggWindow.MONTH:
        return relativedelta(months=length)
    elif window == AggWindow.QUARTER:
        return relativedelta(months=(3*length))
    elif window == AggWindow.YEAR:
        return relativedelta(years=length)
    else:
        return None

fsets = Variable.get('agg_feature_sets', deserialize_json=True, default_var={})
for fset, args in fsets.items():
    dag_id = f'{fset}_Pipeline'
    args['start_date'] = datetime.strptime(args['start_date'], '%Y-%m-%d')
    args['schedule_interval'] = agg_window_to_timedelta(args['schedule_interval'])
    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f'Run incremental pipeline for features in {fset}',
        # schedule_interval=timedelta(days=3),
        # start_date=days_ago(1),
        catchup=False,
        tags=['pipelines'],
        **args
    )

    with dag:
        env = {
            'SPLICE_JUPYTER_USER': env_vars.get('SPLICE_JUPYTER_USER') or env_vars.get('DB_USER'),
            'SPLICE_JUPYTER_PASSWORD': env_vars.get('SPLICE_JUPYTER_PASSWORD') or env_vars.get('DB_PASSWORD'),
            'SPLICE_DB_HOST': env_vars.get('SPLICE_DB_HOST') or env_vars.get('DB_HOST'),
            'SPLICE_KAFKA_HOST': env_vars.get('SPLICE_KAFKA_HOST')
        }

        conf_path = '/mnt/airflow-conf/extra_spark_config.json'
        if path.exists(conf_path):
            with open(conf_path) as f:
                extra_conf = json.load(f)
        else:
            extra_conf = {}

        calculate_statistics_task = SparkSubmitOperator(
            application="/opt/airflow/spark_apps/pipeline.py", 
            task_id="run_pipeline",
            conn_id="splice_spark",
            env_vars=env,
            application_args=[fset],
            **spark_defaults,
            **extra_conf
        )

    globals()[dag_id] = dag
