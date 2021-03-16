from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
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

def create_table(id):
    #Begin spark session 
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    from splicemachine.spark import ExtPySpliceContext
    splice = ExtPySpliceContext(spark, JDBC_URL='jdbc:splice://host.docker.internal:1527/splicedb;user=splice;password=admin', kafkaServers='host.docker.internal:9092')

    table = f'splice.foo{id}'

    if not splice.tableExists(table): 
        df = splice.df('select * from sys.systables')
        splice.createTable(df, table, to_upper=False)
        splice.insert(df, table, to_upper=False)
    return splice.df(f'select * from {table}').count()

fsets = Variable.get('feature_sets', deserialize_json=True)
for id in fsets:
    dag = DAG(
        f'Spark_Test_{id}',
        default_args=default_args,
        description='Test running queries against standalone db',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(2),
        tags=['example'],
    )

    t1 = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        op_kwargs={'id': id},
        dag=dag,
    )