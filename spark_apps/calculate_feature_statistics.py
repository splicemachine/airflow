from os import environ as env_vars, popen
from pyspark.sql import SparkSession
from splicemachine.spark import ExtPySpliceContext
from splicemachine.features import FeatureStore
from pyspark_dist_explore import pandas_histogram
from datetime import datetime
import sys
import numpy as np
import pandas as pd
import json
import logging

spark = SparkSession.builder.\
        config('spark.kubernetes.driver.pod.name', env_vars['POD_NAME']).\
        config('spark.driver.host', popen('hostname -i').read().strip()).\
        getOrCreate()
logging.basicConfig(stream=sys.stdout, level=logging.WARN)
sc = spark.sparkContext
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.warn("pyspark script logger initialized")

def cardinality(row, df):
    name = row['name']
    if row['feature'].is_continuous():
        LOGGER.warn(f'Cannot calculate cardinality for column {name} - cardinality not supported for continuous data types')
        return None
    else:
        df.select(name).distinct().count()

def histogram(row, df, splice):
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
            LOGGER.warn(f"Cannot create histogram for data type {data_type}'")
            return None
        else:
            histogram = df.groupBy(name).count().toPandas().set_index(name)
            histogram = histogram / np.sum(histogram)
            return json.dumps(histogram.to_dict()['count'])

def calculate_statistics(fset):
    user = env_vars['SPLICE_JUPYTER_USER']
    password = env_vars['SPLICE_JUPYTER_PASSWORD']
    db_host = env_vars['SPLICE_DB_HOST']
    kafka_host = env_vars['SPLICE_KAFKA_HOST']

    splice = ExtPySpliceContext(spark, JDBC_URL=f'jdbc:splice://{db_host}:1527/splicedb;user={user};password={password}', kafkaServers=f'{kafka_host}:9092')

    LOGGER.warn(f"Pulling data from feature set '{fset}'")
    df = splice.df(f'select * from {fset}')

    schema, table = fset.split('.')

    fs = FeatureStore()

    LOGGER.warn(f"Getting features in feature set '{fset}'")
    features = fs.get_features_from_feature_set(schema, table)

    update_time = datetime.now()
    
    stats = df.select([f.name for f in features]).summary().toPandas().\
        set_index('summary').T.rename_axis('name').rename_axis(None, axis=1).reset_index()
    cols = list(stats.columns)[1:]
    stats[cols] = stats[cols].apply(pd.to_numeric, errors='coerce')

    stats['feature'] = stats.apply(lambda row: next((f for f in features if f.name.upper() == row['index'].upper()), None), axis=1)
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
    stats['last_update_ts'] = update_time
    stats['last_update_username'] = 'airflow'
    stats.rename(columns=str.upper, inplace=True)
    
    fs_df = splice.df('select * from featurestore.feature_stats')
    stats = stats[fs_df.columns]

    LOGGER.warn(f"Statistics calculated for all features")
    stats_df = splice.createDataFrame(stats, fs_df.schema)
    LOGGER.warn(f"Pushing statistics to FeatureStore.Feature_Stats")
    splice.insert(stats_df, 'FEATURESTORE.FEATURE_STATS')
    LOGGER.warn(f"Statistics pushed to database")
    # This log is necessary for airflow to recognize successful executions
    LOGGER.warn('Exit code: 0')
    spark.stop()

def main():
    fset = sys.argv[1]
    calculate_statistics(fset)

if __name__ == '__main__':
    main()
