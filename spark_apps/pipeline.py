from os import environ as env_vars, popen
from pyspark.sql import SparkSession
from splicemachine.spark import ExtPySpliceContext
from splicemachine.features import FeatureStore
import sys
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

def retrieve_pipeline(splice, schema_name: str, table_name:str):
    """
    Retrieves SQL pipeline definition for feature set @ schema_name.table_name.
    """
    sql = f"""SELECT feature_set_id, source_id, pipeline_start_ts, pipeline_interval, backfill_start_ts, backfill_interval 
              FROM featurestore.pipeline
              WHERE feature_set_id = 
                    (
                        SELECT feature_set_id 
                        FROM featurestore.feature_set 
                        WHERE schema_name='{schema_name}'
                          AND table_name='{table_name}'
                    )
           """
    df = splice.df(sql)
    rows = df.collect()
    if rows:
        return rows[0].asDict()

def retrieve_source(splice, name: str = None, source_id: int = None):
    """
    Reads pipeline source definition from feature store
    One of the two parameters must be specified:
    :param name: The name of the source. Must be unique to other sources
    :param source_id: The system generated id for the source. 
    """
    wname = f" name = '{name}' " if name else ''
    wid = f' source_id = {source_id}' if (source_id and not name) else ''
    sql = f'''SELECT source_id, sql_text, event_ts_column, update_ts_column FROM featurestore.source WHERE {wname}{wid} '''
    source_rows = splice.df(sql).collect()
    if not source_rows:
        return
    source_row = source_rows[0]
    sql = f'''SELECT key_column_name FROM featurestore.source_key WHERE source_id = (select source_id FROM featurestore.source WHERE {wname}{wid})'''
    source_pks = splice.df(sql)
    pks = list(source_pks.select('KEY_COLUMN_NAME').toPandas()['KEY_COLUMN_NAME']) 
    
    source_def = dict(source_id=source_row['SOURCE_ID'], 
                        name = name, 
                        sql = source_row['SQL_TEXT'], 
                        event_ts_col = source_row['EVENT_TS_COLUMN'], 
                        upd_ts_col = source_row['UPDATE_TS_COLUMN'], 
                        pk_cols = pks
                )
    return source_def

def run_incremental_pipeline(fset: str, splice):
    schema_name, table_name = [name.upper() for name in fset.split('.')]

    fs = FeatureStore()

    pipeline = retrieve_pipeline(splice, schema_name = schema_name, table_name = table_name)
    source = retrieve_source(splice, source_id = pipeline['SOURCE_ID'])   
    pipeline_source_sql = fs.get_pipeline_sql(schema_name=schema_name, table_name=table_name)
    LOGGER.warn(f'Incremental data extract: [{pipeline_source_sql}]')
    # ideally the following things should all occur within a single transaction such that the 
    # incremental extract timestamp is only updated once the process completes
    incremental_data = splice.df(pipeline_source_sql)
    max_update_ts = incremental_data.agg({"MAX_UPDATE_TS": "max"}).collect()[0]["max(MAX_UPDATE_TS)"]
    if (max_update_ts == None):
        LOGGER.warn('Nothing new to process.')
        return
    # separate data into history updates - those that are not the most recent ASOF_TS
    # and new updates - those that have the latest ASOF_TS for each entity
    incremental_data = incremental_data.drop('MAX_UPDATE_TS')
    incremental_data.createOrReplaceTempView("Incremental")
    pk_cols = ",".join(source['pk_cols'])
    new_ones = spark.sql( f"SELECT {pk_cols}, MAX(asof_ts) AS ASOF_TS FROM Incremental GROUP BY {pk_cols}")
    history_update_data = incremental_data.join(new_ones, source['pk_cols'] + ['ASOF_TS'], "leftanti")
    new_data = incremental_data.join(new_ones, source['pk_cols'] + ['ASOF_TS'], "inner" ) 
    # updates feature set history with older values
    LOGGER.warn('History update started...')
    splice.mergeInto(history_update_data, f"{schema_name}.{table_name}_history")
    # set newest values in the feature set table
    new_data = new_data.drop("INGEST_TS")
    new_data = new_data.withColumnRenamed( "ASOF_TS", "LAST_UPDATE_TS")
    LOGGER.warn('Current values update started...')
    splice.mergeInto(new_data, f"{schema_name}.{table_name}")
    #update the incremental extract TS
    LOGGER.warn(f'Process complete. Setting next extract threshold to [{max_update_ts}]')
    sql = f"""INSERT INTO featurestore.pipeline_ops( FEATURE_SET_ID, EXTRACT_UP_TO_TS ) --splice-properties insertMode=UPSERT
            VALUES({pipeline['FEATURE_SET_ID']}, timestamp('{max_update_ts}'))"""
    splice.execute(sql)

def main():
    user = env_vars['SPLICE_JUPYTER_USER']
    password = env_vars['SPLICE_JUPYTER_PASSWORD']
    db_host = env_vars['SPLICE_DB_HOST']
    kafka_host = env_vars['SPLICE_KAFKA_HOST']

    splice = ExtPySpliceContext(spark, JDBC_URL=f'jdbc:splice://{db_host}:1527/splicedb;user={user};password={password}', kafkaServers=f'{kafka_host}:9092')
    splice.setAutoCommitOff()
    
    fset = sys.argv[1]
    try:
        run_incremental_pipeline(fset, splice)
        splice.commit()
        # This log is necessary for airflow to recognize successful executions
        LOGGER.warn('Exit code: 0')
    except Exception as e:
        LOGGER.error(e)
        LOGGER.warn('Rolling back...')
        splice.rollback()
        raise e
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
