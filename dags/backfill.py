from datetime import timedelta
from airflow.utils import trigger_rule
from retrying import retry
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, AirflowException

# Operators; we need this to operate!
from airflow.operators.python import task, get_current_context
from airflow.utils.dates import days_ago
from airflow.decorators import dag
from airflow.utils.trigger_rule import TriggerRule
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

def handle_run(sql, interval, event):
    if event.is_set():
        return
    try:
        run_sql(sql, interval)
    except Exception as e:
        event.set()
        raise e

# def check_for_failure(temp_table):
#     c = get_cursor()
#     previous_failure = c.execute(f'select * from featurestore.{temp_table}').fetchone()[0]
#     c.commit()
#     return previous_failure

# def handle_interval(sql, interval, temp_table):
#     if check_for_failure(temp_table):
#         raise Exception(f'Previous backfill interval failed - will not attempt for interval {interval}')
#     try:
#         run_sql(sql, interval)
#     except Exception as e:
#         c = get_cursor()
#         c.execute(f'update featurestore.{temp_table} set failed=true')
#         c.commit()
#         raise e

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
    latest = intervals.pop()
    return {
        "statement": sql,
        "params": intervals,
        "latest": latest
    }

# @task
# def create_temp_table():
#     from uuid import uuid1

#     s = str(uuid1())
#     cursor = get_cursor()

#     cursor.execute(f'create table featurestore.{s} (failed boolean primary key)')
#     cursor.execute(f'insert into featurestore.{s} values false')
#     cursor.commit()

#     return s

def cleanup(schema, table):
    print("Error encountered. Resetting...")
    c = get_cursor()
    print(f'truncating table {schema}.{table}')
    c.execute(f'truncate table {schema}.{table}')
    print(f'truncating table {schema}.{table}_HISTORY')
    c.execute(f'truncate table {schema}.{table}_HISTORY')
    c.commit()

@task
def do_backfill(sql, intervals):
    import multiprocessing as mp
    params = get_current_context()['params']
    schema = params['schema']
    table = params['table']
    
    event = mp.Event()

    args = [(sql, i, event) for i in intervals[:-1]]

    args[3] = ('adsfadfvav', args[3][1])

    serving_sql = sql.replace(f'{schema}.{table}_HISTORY', f'{schema}.{table}').\
        replace('ASOF_TS','LAST_UPDATE_TS').\
        replace('INGEST_TS,', '').\
        replace('CURRENT_TIMESTAMP,','')
    args.append((serving_sql, intervals[-1], event))

    try:
        with mp.Pool(10) as p:
            p.starmap(handle_run, args)
    except Exception as e:
        print(str(e))
        cleanup(schema, table)
        raise AirflowException("An error occurred during the backfill process. "
                                "The Feature Set tables have been cleared.")
# @task
# def populate_serving_table(sql, interval):

#     sql = sql.replace(f'{schema}.{table}_HISTORY', f'{schema}.{table}').\
#             replace('ASOF_TS','LAST_UPDATE_TS').\
#             replace('INGEST_TS,', '').\
#             replace('CURRENT_TIMESTAMP,','')
#     run_sql(sql, interval)

# @task(trigger_rule=TriggerRule.ONE_FAILED)
# def cleanup():
#     params = get_current_context()['params']
#     schema = params['schema']
#     table = params['table']

#     c = get_cursor()
#     print(f'truncating table {schema}.{table}')
#     c.execute(f'truncate table {schema}.{table}')
#     print(f'truncating table {schema}.{table}_HISTORY')
#     c.execute(f'truncate table {schema}.{table}_HISTORY')
#     c.commit()

# @task(retries=0)
# def cause_failure():
#     raise AirflowException("The backfill process failed. Marking DAG as failed...")

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
        do_backfill(sql['statement'], sql['params']) # >> cleanup() >> cause_failure()
        
