from datetime import timedelta
from retrying import retry
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, AirflowException

# Operators; we need this to operate!
from airflow.operators.python import task, get_current_context
from airflow.utils.dates import days_ago
from airflow.decorators import dag
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

def get_cursor():
    from os import environ as env_vars
    from splicemachinesa.pyodbc import splice_connect
    
    user = env_vars['SPLICE_JUPYTER_USER']
    password = env_vars['SPLICE_JUPYTER_PASSWORD']
    host = env_vars['SPLICE_DB_HOST']
    cnx = splice_connect(URL=host, UID=user, PWD=password, SSL=None)
    cursor = cnx.cursor()
    return cursor

@retry(stop_max_attempt_number=3)
def run_sql(sql, interval):
    c = get_cursor()
    print(f'Running insert for interval {interval}')
    c.execute(sql.format(backfill_asof_ts=interval))
    print(f'insert for interval {interval} finished successfully')
    c.commit()
    c.close()

def handle_runs(sql, interval):
    if failed.value:
        return
    try:
        run_sql(sql, interval)
    except Exception as e:
        with failed.get_lock():
            failed.value = True
        raise e

@task(multiple_outputs=True)
def get_sql():
    from splicemachine.features import FeatureStore
    
    params = get_current_context()['params']
    schema = params['schema']
    table = params['table']

    fs = FeatureStore()
    
    print("Getting backfill sql")
    sql = fs.get_backfill_sql(schema, table)
    print("Getting backfill intervals")
    intervals = fs.get_backfill_intervals(schema, table)
    return {
        "statement": sql,
        "params": intervals
    }

def cleanup(schema, table):
    print("Error encountered. Resetting...")
    c = get_cursor()
    print(f'truncating table {schema}.{table}')
    c.execute(f'truncate table {schema}.{table}')
    print(f'truncating table {schema}.{table}_HISTORY')
    c.execute(f'truncate table {schema}.{table}_HISTORY')
    c.commit()

def init_globals(value):
    global failed
    failed = value

@task
def do_backfill(sql, intervals):
    import multiprocessing as mp
    from ctypes import c_bool
    params = get_current_context()['params']
    schema = params['schema']
    table = params['table']

    args = [(sql, i) for i in intervals[:-1]]

    serving_sql = sql.replace(f'{schema}.{table}_HISTORY', f'{schema}.{table}').\
        replace('ASOF_TS','LAST_UPDATE_TS').\
        replace('INGEST_TS,', '').\
        replace('CURRENT_TIMESTAMP,','')
    args.append((serving_sql, intervals[-1]))

    failed = mp.Value(c_bool, False)

    with mp.Pool(10, initializer=init_globals, initargs=(failed,)) as p:
        try:
            p.starmap(handle_runs, args)
        except Exception as e:
            print(str(e))
            cleanup(schema, table)
            raise AirflowException("An error occurred during the backfill process. "
                                    "The Feature Set tables have been cleared.")
dag = DAG(
    'Feature_Set_Backfill',
    default_args=default_args,
    description='Backfills a Feature Set created from a Source',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['backfill']
)

with dag:
    sql = get_sql()
    do_backfill(sql['statement'], sql['params'])
        
