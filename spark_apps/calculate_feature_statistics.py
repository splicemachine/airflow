from os import environ as env_vars, popen
from pyspark.sql import SparkSession
from splicemachine.spark import ExtPySpliceContext
from splicemachine.features import FeatureStore
from splicemachine.features.constants import FeatureType
from pyspark_dist_explore import pandas_histogram
import pyspark.sql.functions as F
from datetime import datetime
import sys
from functools import reduce
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

def median(row, df):
    name = row['index']
    try:
        return df.approxQuantile(name, [0.5], 0)[0]
    except Exception as e:
        LOGGER.warn(f'Cannot calculate median for column {name} - {e}')
        return None

def cardinality(row, df):
    name = row['index']
    if row['feature'].is_continuous():
        LOGGER.warn(f'Cannot calculate cardinality for column {name} - cardinality not supported for continuous data types')
        return None
    else:
        df.select(name).distinct().count()

def histogram(row, df, splice):
    name = row['index']
    feature = row['feature']
    f_id = row['feature_id']

    if feature.is_continuous():
        sql = f'select feature_histogram from featurestore.feature_stats where feature_id = {f_id} order by last_update_ts desc {{limit 1}}'
        previous = splice.df(sql)
        if previous.count() == 0:
            iqr = reduce(lambda x,y: x-y, df.approxQuantile(name, [0.75, 0.25], 0))
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

    # stats = pd.DataFrame(columns=['feature_id', 'feature_cardinality', 'feature_histogram', 'feature_mean', 
    #             'feature_median', 'feature_count', 'feature_stddev', 'last_update_ts', 'last_update_username'])
    update_time = datetime.now()
    
    stats = df.select([f.name for f in features]).summary().toPandas().set_index('summary').T.reset_index()

    stats['feature'] = stats.apply(lambda row: next((f for f in features if f.name.upper() == row['index'].upper()), None), axis=1)
    stats['feature_id'] = stats.apply(lambda row: row['feature'].feature_id, axis=1)
    stats['feature_median'] = stats.apply(lambda row: median(row, df), axis=1)
    stats['feature_cardinality'] = stats.apply(lambda row: cardinality(row, df), axis=1)
    stats['feature_histogram'] = stats.apply(lambda row: histogram(row, df, splice), axis=1)
    
    stats.rename(columns={'count': 'feature_count', 'mean': 'feature_mean', 'stddev': 'feature_stddev'}, inplace=True)
    stats['last_update_ts'] = update_time
    stats['last_update_username'] = 'airflow'
    stats.rename(columns=str.upper, inplace=True)
    
    fs_df = splice.df('select * from featurestore.feature_stats')
    stats = stats[fs_df.columns]
    # for feature in features:
    #     name = feature.name
    #     f_id = feature.feature_id

    #     LOGGER.warn(f"Calculating statistics for feature '{name}'")
    #     count = df.select(name).count()

    #     if feature.is_continuous():
    #         sql = f'select feature_histogram from featurestore.feature_stats where feature_id = {f_id} order by last_update_ts desc {{limit 1}}'
    #         previous = splice.df(sql)
    #         if previous.count() == 0:
    #             iqr = reduce(lambda x,y: x-y, df.approxQuantile(name, [0.75, 0.25], 0))
    #             bin_width = int(2 * (iqr / (count ** (1. / 3))))
    #             f_min = df.select(F.min(df[name])).first()[0]
    #             f_max = df.select(F.max(df[name])).first()[0]
    #             if f_min == f_max:
    #                 bins = []
    #             elif bin_width > 0:
    #                 bins = list(np.arange(f_min, f_max, bin_width))
    #             else:
    #                 bins = list(np.linspace(f_min, f_max, 10, False))
    #             bins = [float('-inf')] + bins + [f_max, float('inf')]
    #             histogram = pandas_histogram(df.select(name), bins=bins)
    #             histogram = histogram / np.sum(histogram)
    #             hist_json = histogram[name].to_json()
    #         else:
    #             old = json.loads(previous.first()[0])
    #             intervals = set()
    #             [intervals.update(key.split(' - ')) for key in old.keys()]
    #             bins = [float(interval) for interval in list(intervals)]
    #             bins.sort()
    #             histogram = pandas_histogram(df.select(name), bins=bins)
    #             histogram = histogram / np.sum(histogram)
    #             hist_json = histogram[name].to_json()
    #     else:
    #         data_type = feature.feature_data_type['data_type'].upper()
    #         if data_type in ['DATE', 'TIME', 'TIMESTAMP']:
    #             LOGGER.warn(f"Cannot create histogram for data type {data_type}'")
    #             hist_json = None
    #         else:
    #             histogram = df.groupBy(name).count().toPandas().set_index(name)
    #             histogram = histogram / np.sum(histogram)
    #             hist_json = json.dumps(histogram.to_dict()['count'])

    #     cardinality = df.select(name).distinct().count()
    #     mean = df.select(F.avg(df[name])).first()[0]
    #     median = df.approxQuantile(name, [0.5], 0)[0]
    #     stddev = df.select(F.stddev(df[name])).first()[0]

    #     row = {
    #         'feature_id': f_id,
    #         'feature_cardinality': cardinality,
    #         'feature_histogram': hist_json,
    #         'feature_mean': mean,
    #         'feature_median': median,
    #         'feature_count': count,
    #         'feature_stddev': stddev,
    #         'last_update_ts': update_time,
    #         'last_update_username': 'airflow'
    #     }
    #     LOGGER.warn(f"Statistics calculated for feature '{name}' - adding to DataFrame")
    #     stats = stats.append(row, ignore_index=True)
    LOGGER.warn(f"Statistics calculated for all features")
    stats_df = splice.createDataFrame(stats, None)
    LOGGER.warn(f"Pushing statistics to FeatureStore.Feature_Stats")
    splice.mergeInto(stats_df, 'FEATURESTORE.FEATURE_STATS')
    LOGGER.warn(f"Statistics pushed to database")
    LOGGER.warn('Exit code: 0')
    spark.stop()

def main():
    fset = sys.argv[1]
    calculate_statistics(fset)

if __name__ == '__main__':
    main()
