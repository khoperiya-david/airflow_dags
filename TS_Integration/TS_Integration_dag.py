from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.hooks.base import BaseHook
import os
import json
import datetime
import logging 
# from bi_operators.email_on_failure import failure_function

# Setting up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)


def get_execution_config(**context):
    
    def _load_sql_query_from_file(file_path):
        with open(file_path, 'r') as f:
            sql_query = f.read()
        return sql_query
    
    
    # получаем параметры подключения к Timesheets
    print('get ts connection config')
    v_web_api_ts = BaseHook.get_connection(context.get('params').get('auth_info_ts_var')) 
    v_web_api_ts_params = json.loads(v_web_api_ts.get_extra())
    
    source_params = {
        "auth_info":v_web_api_ts_params,
        "limit": context['params']['limit'],
        "conversion_added_facts_path":context['params']['conversion_added_facts_path'],
        "conversion_fact_transfers_path":context['params']['conversion_fact_transfers_path'],
        "ts_url":context['params']['ts_url']
        }
    
    v_mssql_olap_staging_dw = BaseHook.get_connection(context['params']['mssql_dwh_var'])
    mssql_dsn = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={v_mssql_olap_staging_dw.host};DATABASE={v_mssql_olap_staging_dw.schema};UID={v_mssql_olap_staging_dw.login};PWD={v_mssql_olap_staging_dw.password};TrustServerCertificate=yes;"
    
    current_dir = os.path.dirname(__file__)
    sql_file_path=os.path.join(current_dir,'etl/sql')
    queries = {}
    for filename in os.listdir(sql_file_path):
        f = os.path.join(sql_file_path, filename)
        if os.path.isfile(f):
            query_name = filename.split('.')[0]
            queries.update({query_name:_load_sql_query_from_file(f)})
    
    dest_params = {
        'mssql_dsn':mssql_dsn,
        'queries':queries
    }
    
    execution_config = {"source_params":source_params, "dest_params":dest_params}

    return execution_config
    

def Load_conversion_added_facts_task(**context):
    from TS_Integration.etl.ts_etl import load_conversion_added_facts
    execution_config = get_execution_config (**context)
    load_conversion_added_facts(**execution_config)
    
def Load_conversion_fact_transfers_task(**context):
    from TS_Integration.etl.ts_etl import load_conversion_fact_transfers
    execution_config = get_execution_config (**context)
    load_conversion_fact_transfers(**execution_config)
    
# default_args ={
#     'on_failure_callback':failure_function
# }

with DAG(
    dag_id="TS_Integration",
    tags=['TS', 'timesheets'],
    description='Load actual data from Timesheest',
    # default_args = default_args,
    render_template_as_native_obj=True,
    schedule='15 6-23/1 * * *',
    max_active_runs = 1,
    start_date=datetime.datetime(2023, 1, 1),
    params={
        "limit": Param(default=10000, type='integer', minimum=1, maximim=10000, title='limit'),
        "ts_url":Param(default='https://api.timesheets.mts.ru', type='string', title='ts_url'),
        "conversion_added_facts_path":Param(default='api/ConversionAddedFacts', type='string', title='conversion_added_facts_path'),
        "conversion_fact_transfers_path":Param(default='api/ConversionFactTransfers', type='string', title='conversion_fact_transfers_path'),
        "auth_info_ts_var":Param(default='v_web_api_timesheets', type='string', title='Vault connection name for timesheets auth'),
        "mssql_dwh_var":Param(default='v_mssql_olap_staging_dw', type='string', title='Vault connection name for mssql dwh connection')
    }) as dag:
    load_ts_conversion_added_facts = PythonOperator(
        task_id='Load_conversion_added_facts',
        python_callable=Load_conversion_added_facts_task
    )
    load_ts_conversion_fact_transfers = PythonOperator(
        task_id='Load_conversion_fact_transfers',
        python_callable=Load_conversion_fact_transfers_task
    )
   
load_ts_conversion_added_facts>>load_ts_conversion_fact_transfers
    