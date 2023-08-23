import airflow
import logging
import json
import requests
import pandas as pd    

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from sqlalchemy.engine import create_engine
from contextlib import closing
from bi_operators.email_on_failure import failure_function


log = logging.getLogger(__name__)

DWH_Loading_MSSQL_ID = "DWH_Loading_MSSQL"
POSTGRES_CONN_ID = "v_postgresql_bi_dwh"

default_args ={
    'on_failure_callback':failure_function
}

dag_params = {
    'dag_id': 'HRGate_Units',
    'schedule_interval':'0 1 * * *',
    'start_date':datetime(2023, 1, 1),
    'tags':["HRGate Integration"],    
    'default_args':default_args,
    'description':'Load HRGate Units'
}

def insert_data():
    from pangres import upsert
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    bi_dwh_source = MsSqlHook(DWH_Loading_MSSQL_ID)
    bi_dwh_conn = bi_dwh_source.get_conn()
    bi_dwh_cursor = bi_dwh_conn.cursor()
    bi_dwh_cursor.execute("exec pr_GetSystemUserInfo 'HRGate Prom'")
    connect_settings = bi_dwh_cursor.fetchone()

    if connect_settings is None:
        raise AirflowFailException("Не удалось получить данные для isso auth")
    client_id = connect_settings[0]
    client_secret= connect_settings[1]
    isso_url = "https://isso.mts.ru/auth/realms/mts/protocol/openid-connect/token"
    app_url = "https://hr-gate.mts.ru/api/v1/units"
    
    query= {
                'limit': 1000,
                'offset': 0,
                'withDeleted':'true'
            }

    target = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_bi_dwh = create_engine(target.get_uri())
    data = []
  
    token = get_token(client_id,client_secret,isso_url)  
    api_data = get_api_data(token, app_url, query, client_id, client_secret, isso_url) 
    while api_data is not None and hasattr(api_data,'values'):
        items = api_data.values;
        for row in items:
            rec = (row[1],row[5],row[9],row[13],row[15])
            data.append(rec)
        query['offset'] =  query['offset']+query['limit']
        api_data = get_api_data(token, app_url, query, client_id, client_secret, isso_url) 

    df = pd.DataFrame(data,
                      columns=['id','description', 'org_code', 'short_name', 'title'])
    df.set_index(['id'], inplace=True, drop=True)
    upsert(con= pg_bi_dwh,
           df=df,
           schema='stg',
           table_name='hrgate_units',
           if_row_exists='update',
           create_table=False)



def get_token(client_id, client_secret, isso_url):
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'auth_url': isso_url,
        'scope': 'hr-gate'
        }
    response = requests.post(isso_url, data=data)
    result = json.loads(response.content)
    token= str(result['access_token'])
    return token

def get_api_data(token, app_url, query, client_id, client_secret, isso_url):
    headers = {
        'Authorization': "Bearer " + token, 
        'client_id': client_id,
        'client_secret': client_secret,
        'auth_url': isso_url,
        'scope': 'hr-gate',
        'Content-Type': 'application/json'
    }
    response = requests.get(app_url, headers=headers, params=query)
    result = json.loads(response.content)
    data = pd.json_normalize(result)
    return data

def exec_sql(**kwargs):
    connection = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_bi_dwh = create_engine(connection.get_uri())
    pg_bi_dwh.execute(kwargs['sql'])

with DAG(**dag_params) as dag:  
                        
    truncate_stg_table = PythonOperator(
        task_id='truncate_stg_hrgate_units',
        python_callable=exec_sql,
        op_kwargs={'sql': 'truncate table stg.hrgate_units;'}
    )

    insert_data_stg = PythonOperator(
        task_id='insert_stg_hrgate_units',
        python_callable=insert_data
    )

    merge_to_dwh = PythonOperator(
        task_id='merge_hrgate_units',
        python_callable=exec_sql,
        op_kwargs={'sql': 'call stg.pr_hrgate_units_actualization();'}
    )

truncate_stg_table >> insert_data_stg >> merge_to_dwh
