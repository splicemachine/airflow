from datetime import timedelta, datetime
from os import environ as env_vars, popen
import json
from pyspark.sql import SparkSession
from splicemachine.spark import ExtPySpliceContext
from splicemachine.features import FeatureStore
from datetime import datetime
import sys
import numpy as np
import pandas as pd
import logging
import re

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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

def cardinality(row, df):
    name = row['name']
    if row['feature'].is_continuous():
        print(f'Cannot calculate cardinality for column {name} - cardinality not supported for continuous data types')
        return None
    else:
        df.select(name).distinct().count()

def histogram(row, df, splice):
    from pyspark_dist_explore import pandas_histogram

    name = row['name']
    feature = row['feature']
    f_id = row['feature_id']

    if feature.is_continuous():
        sql = f'select feature_histogram from featurestore.feature_stats where feature_id = {f_id} order by last_update_ts desc {{limit 1}}'
        previous = splice.df(sql)
        if previous.count() == 0:
            iqr = row['75%'] - row['25%']
            bin_width = int(2 * (iqr / (row['count'] ** (1. / 3))))
            f_min = row['min']
            f_max = row['max']
            if f_min == f_max:
                bins = []
            elif bin_width > 0:
                bins = list(np.arange(f_min, f_max, bin_width))
            else:
                bins = list(np.linspace(f_min, f_max, 10, False))
            bins = [float('-inf')] + bins + [f_max, float('inf')]
        else:
            old = json.loads(previous.first()[0])
            intervals = set()
            [intervals.update(key.split(' - ')) for key in old.keys()]
            bins = [float(interval) for interval in list(intervals)]
            bins.sort()
        histogram = pandas_histogram(df.select(name), bins=bins)
        histogram = histogram / np.sum(histogram)
        return histogram[name].to_json()
    else:
        data_type = feature.feature_data_type['data_type'].upper()
        if data_type in ['DATE', 'TIME', 'TIMESTAMP']:
            print(f"Cannot create histogram for data type {data_type}'")
            return None
        else:
            histogram = df.groupBy(name).count().toPandas().set_index(name)
            histogram = histogram / np.sum(histogram)
            return json.dumps(histogram.to_dict()['count'])

@task
def calculate_statistics(fset, fsid, fsv):
    spark = SparkSession.builder.\
        config('spark.kubernetes.driver.pod.name', env_vars['POD_NAME']).\
        config('spark.driver.host', popen('hostname -i').read().strip()).\
        getOrCreate()
    logging.basicConfig(stream=sys.stdout, level=logging.WARN)
    # sc = spark.sparkContext
    # log4jLogger = sc._jvm.org.apache.log4j
    # LOGGER = log4jLogger.LogManager.getLogger(__name__)
    # LOGGER.warn("pyspark script logger initialized")

    db_host = env_vars.get('SPLICE_DB_HOST') or env_vars.get('DB_HOST')
    user = env_vars.get('SPLICE_JUPYTER_USER') or env_vars.get('DB_USER')
    password = env_vars.get('SPLICE_JUPYTER_PASSWORD') or env_vars.get('DB_PASSWORD')
    kafka_host = env_vars.get('SPLICE_KAFKA_HOST')
    splice = ExtPySpliceContext(spark, JDBC_URL=f'jdbc:splice://{db_host}:1527/splicedb;user={user};password={password}', kafkaServers=f'{kafka_host}:9092')
    splice.setAutoCommitOff()

    print(f"Pulling data from feature set '{fset}'")
    df = splice.df(f'select * from {fset}')

    schema, table = fset.split('.')
    table = re.sub(f'_v{fsv}$', '', table)

    fs = FeatureStore()

    print(f"Getting features in feature set '{fset}'")
    features = fs.get_features_from_feature_set(schema, table)

    update_time = datetime.now()
    
    stats = df.select([f.name for f in features]).summary().toPandas().\
        set_index('summary').T.rename_axis('name').rename_axis(None, axis=1).reset_index()

    if not int(max(stats['count'])):
        print('nothing to see here')
        return
    
    cols = list(stats.columns)[1:]
    stats[cols] = stats[cols].apply(pd.to_numeric, errors='coerce')

    stats['feature'] = stats.apply(lambda row: next((f for f in features if f.name.upper() == row['name'].upper()), None), axis=1)
    stats['feature_id'] = stats.apply(lambda row: row['feature'].feature_id, axis=1)
    stats['feature_cardinality'] = stats.apply(lambda row: cardinality(row, df), axis=1)
    stats['feature_histogram'] = stats.apply(lambda row: histogram(row, df, splice), axis=1)
    
    rename = {
        'count': 'feature_count',
        'mean': 'feature_mean',
        'stddev': 'feature_stddev',
        '50%': 'feature_median',
        '25%': 'feature_q1',
        '75%': 'feature_q3',
        'min': 'feature_min',
        'max': 'feature_max'
    }
    stats.rename(columns=rename, inplace=True)
    stats['feature_set_id'] = fsid
    stats['feature_set_version'] = fsv
    stats['last_update_ts'] = update_time
    stats['last_update_username'] = 'airflow'
    stats.rename(columns=str.upper, inplace=True)
    
    fs_df = splice.df('select * from featurestore.feature_stats')
    stats = stats[fs_df.columns]

    print(f"Statistics calculated for all features")
    stats_df = splice.createDataFrame(stats, fs_df.schema)
    print(f"Pushing statistics to FeatureStore.Feature_Stats")
    splice.insert(stats_df, 'FEATURESTORE.FEATURE_STATS')
    print(f"Statistics pushed to database")
    splice.commit()

fsets = Variable.get('feature_sets', deserialize_json=True, default_var={})
for fset, args in fsets.items():
    dag_id = f'SM_{fset}_Feature_Statistics'
    args['start_date'] = datetime.strptime(args['start_date'], '%Y-%m-%d')
    op_args = args.pop('op_args')
    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f'Calculate feature statistics for features in {fset}',
        # schedule_interval='@daily',
        # start_date=days_ago(1),
        catchup=False,
        tags=['statistics'],
        **args
    )

    with dag:
        # env = {
        #     'SPLICE_JUPYTER_USER': env_vars.get('SPLICE_JUPYTER_USER') or env_vars.get('DB_USER'),
        #     'SPLICE_JUPYTER_PASSWORD': env_vars.get('SPLICE_JUPYTER_PASSWORD') or env_vars.get('DB_PASSWORD'),
        #     'SPLICE_DB_HOST': env_vars.get('SPLICE_DB_HOST') or env_vars.get('DB_HOST'),
        #     'SPLICE_KAFKA_HOST': env_vars.get('SPLICE_KAFKA_HOST')
        # }

        # calculate_statistics_task = SparkSubmitOperator(
        #     application="/opt/airflow/spark_apps/calculate_feature_statistics.py", 
        #     task_id="calculate_statistics",
        #     conn_id="splice_spark",
        #     env_vars=env,
        #     application_args=[fset, *op_args]
        # )
        calculate_statistics(fset, *op_args)

    globals()[dag_id] = dag
