from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.utils.dates import days_ago
from airflow.decorators import dag
from airflow.operators.python import PythonVirtualenvOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
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

def run_spark():
    from os import environ as env_vars, popen, path
    import json
    from pyspark.sql import SparkSession
    # eNSDS_jar = 'https://splice-releases.s3.amazonaws.com/3.1.0.2009/cluster/nsds/splice_spark2-3.1.0.2009-shaded-dbaas3.0.jar'
    conf_path = '/mnt/airflow-conf/extra_spark_config.json'
    if path.exists(conf_path):
        with open(conf_path) as f:
            extra_conf = json.load(f)
    else:
        extra_conf = {}

    spark = SparkSession.builder.\
        config('spark.kubernetes.driver.pod.name', env_vars['POD_NAME']).\
        config('spark.driver.host', popen('hostname -i').read().strip()).\
        config('spark.files', extra_conf['files'])
    for key, value in extra_conf['conf'].items():
        spark = spark.config(key, value)
    spark = spark.getOrCreate()

    print(spark.sparkContext.getConf().getAll())

    import pandas as pd
    pdf = pd.DataFrame([[1,'foo'],[2,'bar'],[3,'baz']])
    df = spark.createDataFrame(pdf)
    df.show()
    df.collect()

dag = DAG(
    'Spark_Test',
    default_args=default_args,
    description='spark',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['spark']
)

with dag:
    task = PythonVirtualenvOperator(
        task_id='show_df',
        python_callable=run_spark,
        python_version='3.7',
        requirements=['dill'],
        use_dill=True
    )
