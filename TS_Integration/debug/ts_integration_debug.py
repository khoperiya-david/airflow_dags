import json
import logging
import os
import pyodbc
import sys
from datetime import datetime
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import etl.ts_etl as etl

def _get_auth(system_name:str):
    # source = MsSqlHook(DWH_LOADING_MSSQL_ID)
    conn  = pyodbc.connect('DRIVER={SQL Server}; SERVER=0411pmobidwh02.pv.mts.ru; DATABASE=OLAP_Loading;Trusted_Connection=yes;}',autocommit=True)
    cursor = conn.cursor()
    cursor.execute(f"exec dbo.pr_GetSystemUserInfo '{system_name}'")
    connect_settings = cursor.fetchone()
    cursor.close()
    conn.close()
    return connect_settings

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def get_dag_config(**context):
    
    def _load_sql_query_from_file(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_query = f.read()
        return sql_query
    
    
    # получаем параметры подключения к ts
    v_web_api_ts =_get_auth('ISSO') #BaseHook.get_connection('v_web_api_ts') 
    v_web_api_ts_params = {
        'access_token_url':'https://isso.mts.ru/auth/realms/mts/protocol/openid-connect/token' ,
        'grant_type': 'client_credentials',
        'client_id':v_web_api_ts[0],
        'client_secret':v_web_api_ts[1]
        } #v_web_api_ts.extra
    
    source_params = {
        "auth_info":
            {
                "auth_url": v_web_api_ts_params.get('access_token_url'),
                "grant_type": v_web_api_ts_params.get('grant_type'),
                "verify_ssl": False,
                "client_id":v_web_api_ts_params.get('client_id'),
                "client_secret":v_web_api_ts_params.get('client_secret'),
                "headers": {
                        "Accept": "application/json",
                        "content-type": "application/x-www-form-urlencoded"
                        }
            },
        "limit": context.get('limit'),
        "conversion_added_facts_path":context.get('conversion_added_facts_path'),
        "conversion_fact_transfers_path":context.get('conversion_fact_transfers_path'),
        "ts_url":context.get('ts_url')
        }
    
    mssql_auth = _get_auth('DWH\olap_staging_dw') # BaseHook.get_connection('v_mssql_olap_staging_dw')
    v_mssql_olap_staging_dw = {'host':'0411pmobidwh02'
                       ,"port": "1433"
                       ,"schema": "OLAP_STAGING_DW"
                       , 'login':mssql_auth[0]
                       , 'password':mssql_auth[1]
                       }
    mssql_dns = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={v_mssql_olap_staging_dw.get('host')};DATABASE={v_mssql_olap_staging_dw.get('schema')};UID={v_mssql_olap_staging_dw.get('login')};PWD={v_mssql_olap_staging_dw.get('password')};TrustServerCertificate=yes;"
    
    current_dir = os.path.dirname(__file__)
    sql_file_path=os.path.join( os.path.dirname(current_dir),'etl/sql')
    queries = {}
    for filename in os.listdir(sql_file_path):
        f = os.path.join(sql_file_path, filename)
        if os.path.isfile(f):
            query_name = filename.split('.')[0]
            queries.update({query_name:_load_sql_query_from_file(f)})
    
    dest_params = {
        'mssql_dsn':mssql_dns,
        'queries':queries
    }
    
    execution_config = {"source_params":source_params, "dest_params":dest_params}

    return execution_config

default_params={
        "ts_url":'https://api.timesheets.mts.ru',
        "conversion_added_facts_path":'api/ConversionAddedFacts'
    }
params = get_dag_config(**default_params)
# print(params)
print(datetime.now())
etl.load_conversion_added_facts(**params)
print(datetime.now())

default_params={
        "ts_url":'https://api.timesheets.mts.ru',
        "conversion_fact_transfers_path":'api/ConversionFactTransfers'
    }
params = get_dag_config(**default_params)
# print(params)
print(datetime.now())
etl.load_conversion_fact_transfers(**params)
print(datetime.now())


