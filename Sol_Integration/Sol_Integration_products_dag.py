from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.hooks.base import BaseHook
import os
import json
import datetime
import logging 
from bi_operators.email_on_failure import failure_function

# Setting up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)


def get_execution_config(**context):
    
    def _load_sql_query_from_file(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_query = f.read()
        return sql_query
    
    
    # получаем параметры подключения к sol
    print('get sol connection config')
    v_web_api_sol = BaseHook.get_connection(context['params']['auth_info_sol_var']) 
    v_web_api_sol_params = json.loads(v_web_api_sol.get_extra())
    
    source_params = {
        "auth_info":
            {
                "auth_url": v_web_api_sol_params.get('access_token_url'),
                "grant_type": v_web_api_sol_params.get('grant_type'),
                "verify_ssl": True,
                "client_id":v_web_api_sol_params.get('client_id'),
                "client_secret":v_web_api_sol_params.get('client_secret'),
                "headers": {
                        "Accept": "application/json",
                        "content-type": "application/x-www-form-urlencoded"
                        }
            },
        "relative_path":context.get('params',{}).get('relative_path'),
        "sol_url":context.get('params',{}).get('sol_url'),
        'types':['TechnicalRecord','Application','Platform','Module','Service','ITService'],
        'sol_classifiers_path':context.get('params',{}).get('sol_classifiers_path'),
        'sol_consumptions_path':context.get('params',{}).get('sol_consumptions_path')
        }
    
    
    v_mssql_olap_staging_dw = BaseHook.get_connection(context['params']['mssql_dwh_var'])
    mssql_dsn = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={v_mssql_olap_staging_dw.host};DATABASE={v_mssql_olap_staging_dw.schema};UID={v_mssql_olap_staging_dw.login};PWD={v_mssql_olap_staging_dw.password};TrustServerCertificate=yes;"
    
    v_postgre_bi_dwh = BaseHook.get_connection(context['params']['pg_dwh_var'])
    pg_dsn = pg_dsn = f"DRIVER={{PostgreSQL Unicode}};SERVER={v_postgre_bi_dwh.host};PORT=5432;DATABASE={v_postgre_bi_dwh.schema};UID={v_postgre_bi_dwh.login};PWD={v_postgre_bi_dwh.password};"
   
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
        'pg_dsn':pg_dsn,
        'queries':queries
    }
    
    execution_config = {"source_params":source_params, "dest_params":dest_params}

    return execution_config
    

def load_products_task(**context):
    from Sol_Integration.etl.sol_etl import load_products
    execution_config = get_execution_config (**context)
    load_products(**execution_config)
    
def load_consumptions_task(**context):
    from Sol_Integration.etl.sol_etl import load_consumptions
    execution_config = get_execution_config (**context)
    load_consumptions(**execution_config)
    
default_args ={
    'on_failure_callback':failure_function
}

with DAG(
    dag_id="SOL_Products",
    tags=['sol', 'products'],
    description='Load products from sol to DWH mssql, postgres',
    default_args = default_args,
    render_template_as_native_obj=True,
    schedule='0 6-23/1 * * *',
    max_active_runs = 1,
    start_date=datetime.datetime(2023, 1, 1),
    params={
        "sol_url":Param(default='https://sol.mts.ru', type='string', title='sol_url'),
        "relative_path":Param(default='api/modules', type='string', title='relative_path'),
        "sol_classifiers_path":Param(default='api/Classifiers/Classifications', type='string', title='sol_classifiers_path'),
        "sol_consumptions_path":Param(default='api/Consumptions', type='string', title='sol_consumptions_path'),
        "auth_info_sol_var":Param(default='v_web_api_sol', type='string', title='Vault connection name for sol auth'),
        "mssql_dwh_var":Param(default='v_mssql_olap_staging_dw', type='string', title='Vault connection name for mssql dwh connection'),
        "pg_dwh_var":Param(default='v_postgre_bi_dwh', type='string', title='Vault connection name for pg dwh connection')
    }) as dag:
    load_sol_products = PythonOperator(
        task_id='Load_products',
        python_callable=load_products_task
    )
    load_sol_consumptions = PythonOperator(
        task_id='Load_consumptions',
        python_callable=load_consumptions_task
    )
   
load_sol_products>>load_sol_consumptions
    