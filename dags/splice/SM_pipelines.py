from datetime import timedelta, datetime
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from pyspark.sql import context
from splicemachine.features import FeatureStore
from os import environ as env_vars

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

fs = FeatureStore()
user = env_vars.get('SPLICE_JUPYTER_USER') or env_vars.get('DB_USER'),
password = env_vars.get('SPLICE_JUPYTER_PASSWORD') or env_vars.get('DB_PASSWORD')
fs.login_fs(user, password)

try:
    pipelines = fs._get_deployed_pipelines()
    for pl in pipelines:
        dag = DAG(
            f'SM_{pl.name}_v{pl.pipeline_version}',
            default_args=default_args,
            description=pl.description,
            schedule_interval=pl.pipeline_interval,
            start_date=datetime.strptime(pl.pipeline_start_date, '%Y-%m-%d'),
            tags=['pipeline']
        )

        with dag:
            @task
            def push_to_feature_set(table, feature_set):
                from splicemachine.spark import ExtPySpliceContext
                from os import environ as env_vars, popen
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.\
                    config('spark.kubernetes.driver.pod.name', env_vars['POD_NAME']).\
                    config('spark.driver.host', popen('hostname -i').read().strip()).\
                    getOrCreate()
                import logging
                import sys
                logging.basicConfig(stream=sys.stdout, level=logging.WARN)
                # sc = spark.sparkContext
                # log4jLogger = sc._jvm.org.apache.log4j
                # LOGGER = log4jLogger.LogManager.getLogger(__name__)

                db_host = env_vars.get('SPLICE_DB_HOST') or env_vars.get('DB_HOST')
                user = env_vars.get('SPLICE_JUPYTER_USER') or env_vars.get('DB_USER')
                password = env_vars.get('SPLICE_JUPYTER_PASSWORD') or env_vars.get('DB_PASSWORD')
                kafka_host = env_vars.get('SPLICE_KAFKA_HOST')
                splice = ExtPySpliceContext(spark, JDBC_URL=f'jdbc:splice://{db_host}:1527/splicedb;user={user};password={password}', kafkaServers=f'{kafka_host}:9092')

                print(f'Pushing results to feature set {feature_set}')
                splice.insert(splice.df(f'select * from {table}'), feature_set)
                print(f'Deleting temp table {table} created by previous pipe')
                splice.dropTable(table)

            table = None
            for pipe in pl.pipes:
                @task.virtualenv(task_id=pipe.name)
                def task_func(lang, ptype, func, op_args, op_kwargs, table=None,
                    schedule_interval=None, execution_date=None, prev_execution_date_success=None):
                    import string
                    import random
                    import cloudpickle
                    from splicemachine.features import PipeLanguage, PipeType
                    from splicemachine.spark import ExtPySpliceContext
                    from os import environ as env_vars, popen
                    from pyspark.sql import SparkSession

                    spark = SparkSession.builder.\
                        config('spark.kubernetes.driver.pod.name', env_vars['POD_NAME']).\
                        config('spark.driver.host', popen('hostname -i').read().strip()).\
                        getOrCreate()
                    print(spark.sparkContext.getConf().getAll())
                    import logging
                    import sys
                    logging.basicConfig(stream=sys.stdout, level=logging.WARN)
                    # sc = spark.sparkContext
                    # log4jLogger = sc._jvm.org.apache.log4j
                    # LOGGER = log4jLogger.LogManager.getLogger(__name__)

                    db_host = env_vars.get('SPLICE_DB_HOST') or env_vars.get('DB_HOST')
                    user = env_vars.get('SPLICE_JUPYTER_USER') or env_vars.get('DB_USER')
                    password = env_vars.get('SPLICE_JUPYTER_PASSWORD') or env_vars.get('DB_PASSWORD')
                    kafka_host = env_vars.get('SPLICE_KAFKA_HOST')
                    splice = ExtPySpliceContext(spark, JDBC_URL=f'jdbc:splice://{db_host}:1527/splicedb;user={user};password={password}', kafkaServers=f'{kafka_host}:9092')

                    ctx = {
                        'schedule_interval': schedule_interval,
                        'execution_date': execution_date,
                        'prev_execution_date_success': prev_execution_date_success
                    }

                    new_table = ''.join(random.choice(string.ascii_letters) for _ in range(10))
                    temp_table = f'splice.{new_table}'

                    op_args = cloudpickle.loads(op_args)
                    op_kwargs = cloudpickle.loads(op_kwargs)
                    f = cloudpickle.loads(func)

                    f.__globals__['context'] = ctx
                    if lang == PipeLanguage.pyspark:
                        f.__globals__['splice'] = splice
                        f.__globals__['spark'] = spark

                    source = None
                    if ptype == PipeType.source:
                        df = f(*op_args, **op_kwargs)
                    else:
                        source = table
                        if not lang == PipeLanguage.sql:
                            table = splice.df(f'select * from {table}')
                            if lang == PipeLanguage.python:
                                table = table.toPandas()
                        print('Calling pipe function')
                        df = f(table, *op_args, **op_kwargs)
                        print('Function executed successfully')
                        
                    if lang == PipeLanguage.sql:
                        df = splice.df(df)
                    elif lang == PipeLanguage.python:
                        df = splice.pandasToSpark(df)

                    print(f'Pushing the following dataframe to temporary table {temp_table}:')
                    df.show()
                    splice.createAndInsertTable(df, temp_table)
                    
                    if source:
                        print(f'Deleting temp table {source} created by previous pipe')
                        splice.dropTable(source)
                    return temp_table

                table = task_func(pipe.lang, pipe.type, pipe.func, pipe._args, pipe._kwargs, table, dag.normalized_schedule_interval)
            
            push_to_feature_set(table, pl.feature_set)
            
except IOError:
    print('Could not connect to Feature Store')
