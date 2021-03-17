from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.decorators import dag, task
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

@task
def create_table(fset: str):
    #Begin spark session 
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    from splicemachine.spark import ExtPySpliceContext
    splice = ExtPySpliceContext(spark, JDBC_URL='jdbc:splice://host.docker.internal:1527/splicedb;user=splice;password=admin', kafkaServers='host.docker.internal:9092')

    schema, table = fset.split('.')

    test_table = f'splice.{table}_test'

    if not splice.tableExists(test_table): 
        df = splice.df('select * from sys.systables')
        splice.createTable(df, test_table, to_upper=False)
        splice.insert(df, test_table, to_upper=False)
    return splice.df(f'select * from {test_table}').count()

fsets = Variable.get('feature_sets', deserialize_json=True, default_var={})
for fset, args in fsets.items():
    dag_id = f'Spark_Test_{fset}'
    dag = DAG(
        dag_id,
        default_args=default_args,
        description='Test running queries against standalone db',
        # schedule_interval='@daily',
        start_date=days_ago(2),
        tags=['example'],
        **args
    )

    with dag:    
    # t1 = PythonOperator(
    #     task_id='create_table',
    #     python_callable=create_table,
    #     op_kwargs={'fset': fset},
    #     dag=dag,
    # )
        create_table(fset)

    globals()[dag_id] = dag