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

    stats = pd.DataFrame(columns=['feature_id', 'feature_cardinality', 'feature_histogram', 'feature_mean', 
                'feature_median', 'feature_count', 'feature_stddev', 'last_update_ts', 'last_update_username'])
    update_time = datetime.now()
    
    for feature in features:
        name = feature.name
        f_id = feature.feature_id

        if feature.feature_type != FeatureType.continuous:
            LOGGER.warn(f"Statistics for feature '{name}' cannot be calculated because the feature is not continuous - skipping")
            continue

        LOGGER.warn(f"Calculating statistics for feature '{name}'")
        count = df.select(name).count()

        sql = f'select feature_histogram from featurestore.feature_stats where feature_id = {f_id} order by last_update_ts desc {{limit 1}}'
        previous = splice.df(sql)
        if previous.count() == 0:
            iqr = reduce(lambda x,y: x-y, df.approxQuantile(name, [0.75, 0.25], 0))
            bin_width = int(2 * (iqr / (count ** (1. / 3))))
            f_min = df.select(F.min(df[name])).first()[0]
            f_max = df.select(F.max(df[name])).first()[0]
            if f_min == f_max:
                bins = []
            elif bin_width > 0:
                bins = list(np.arange(f_min, f_max, bin_width))
            else:
                bins = list(np.linspace(f_min, f_max, 10, False))
            bins = [float('-inf')] + bins + [f_max, float('inf')]
            histogram = pandas_histogram(df.select(name), bins=bins)[name].to_json()
        else:
            old = json.loads(previous.first()[0])
            intervals = set()
            [intervals.update(key.split(' - ')) for key in old.keys()]
            bins = [float(interval) for interval in list(intervals)]
            bins.sort()
            histogram = pandas_histogram(df.select(name), bins=bins)[name].to_json()

        cardinality = df.select(name).distinct().count()
        mean = df.select(F.avg(df[name])).first()[0]
        median = df.approxQuantile(name, [0.5], 0)[0]
        stddev = df.select(F.stddev(df[name])).first()[0]

        row = {
            'feature_id': f_id,
            'feature_cardinality': cardinality,
            'feature_histogram': histogram,
            'feature_mean': mean,
            'feature_median': median,
            'feature_count': count,
            'feature_stddev': stddev,
            'last_update_ts': update_time,
            'last_update_username': ''
        }
        LOGGER.warn(f"Statistics calculated for feature '{name}' - adding to DataFrame")
        stats = stats.append(row, ignore_index=True)
    LOGGER.warn(f"Statistics calculated for all features")
    stats_df = splice.createDataFrame(stats, None)
    LOGGER.warn(f"Pushing statistics to FeatureStore.Feature_Stats")
    splice.insert(stats_df, 'FEATURESTORE.FEATURE_STATS', to_upper=False)
    LOGGER.warn(f"Statistics pushed to database")
    LOGGER.warn('Exit code: 0')
    spark.stop()

def main():
    fset = sys.argv[1]
    calculate_statistics(fset)

if __name__ == '__main__':
    main()
