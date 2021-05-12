from datetime import timedelta
import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.decorators import dag, task
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

fsets = Variable.get('feature_sets', deserialize_json=True, default_var={})
for fset, args in fsets.items():
    dag_id = f'{fset}_Calculate_Feature_Statistics'
    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f'Calculate feature statistics for features in {fset}',
        # schedule_interval='@daily',
        start_date=days_ago(1),
        catchup=False,
        tags=['statistics'],
        **args
    )

    with dag:
        calculate_statistics_task = SparkSubmitOperator(
            application="/opt/airflow/spark_apps/calculate_feature_statistics.py", 
            task_id="calculate_statistics",
            conn_id="splice_spark",
            env_vars={k: os.environ[k] for k in ['SPLICE_JUPYTER_USER', 'SPLICE_JUPYTER_PASSWORD', 'SPLICE_DB_HOST', 'SPLICE_KAFKA_HOST']},
            application_args=[fset],
            **spark_defaults
            # conf={"spark.driver.extraJavaOptions": "-Dlog4j.configuration=file:spark.log4j.properties"},
            # files="/etc/spark/conf/spark.log4j.properties"
        )

    globals()[dag_id] = dag
