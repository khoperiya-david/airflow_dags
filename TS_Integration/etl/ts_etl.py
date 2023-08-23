import json
import requests
from datetime import datetime, timedelta
import base64
import pyodbc
import gzip
import asyncio
import aiohttp
import aioodbc
from numpy import array_split
from math import ceil
import itertools
requests.packages.urllib3.disable_warnings()

def _divide_chunks(l:list, n:int):
    return array_split(l, n)

def _get_token(auth_info):
    # полуение токена авторизации
    # на вход подается json с информацией необходимой для получения токена
    '''
    {
        "auth_info": {
            "auth_url": "https://isso-dev.mts.ru/auth/realms/mts/protocol/openid-connect/token",
            "grant_type": "client_credentials",
            "scope": "scope",
            "client_id": "some_clientid",
            "client_secret": "some_client_secret",
            "verify_ssl": false,
            "headers": {
                "Accept": "application/json",
                "content-type": "application/x-www-form-urlencoded"
            }
        }
    }
    '''
# функция возвращает токен и датувремя срока истечения его годности
# 
# 
    url = auth_info['auth_url']
    post_content = {
        "grant_type": auth_info['grant_type'],
        "client_id": auth_info['client_id'],
        "client_secret": auth_info['client_secret']
    }
    if "scope" in auth_info:
        post_content["scope"] = auth_info['scope']
    
    # фиксируем вемя получения токена
    token_received_at = datetime.now()
    response = requests.post(url=url,
                             data=post_content,
                             headers=auth_info['headers'],
                             verify=auth_info['verify_ssl']
                             ).json()
    # возвращаем сам токен и время истечения срока годности
    token_info = {
        'token': response['access_token'],
        'expires_at': token_received_at + timedelta(seconds=response['expires_in']-30)
    }
    return token_info

def _save_to_db_mssql(data, conn, entity):

    def _compress_data_for_mssql(data):
        templ = '0x1F8B0800000000000400'
        compressed_value = gzip.compress(bytes(json.dumps(data), 'utf-16LE'))
        out = templ + compressed_value.hex()[20:]
        return out
    
    binary = _compress_data_for_mssql(data)
    sql_query = f"""
    declare @val AS varbinary(max) = {binary}
    DECLARE @JsonValue nvarchar(max) = CONVERT (nvarchar(MAX), DECOMPRESS (@val))
    INSERT INTO [stg].[TS_Json]
    (
        Entity,
        JsonText
    )
    SELECT '{entity}', @JsonValue
    """

    with conn.cursor() as cur:
        cur.execute(sql_query)
        cur.commit()
        
def load_conversion_fact_transfers(**kwargs):
    def _extract_conversion_fact_transfers(data):
        resp  = [
            {
            "factTransferId":x.get("factTransferId"),
            "conversionPeriodId":x.get("conversionPeriodId"),
            "projectFrom":x.get("projectFrom"),
            "projectTo":x.get("projectTo"),
            "coefficient":x.get("coefficient"),
            "createdAt":x.get("createdAt"),
            "rmsLegalEntityId":x.get("rmsLegalEntityId") 
            }
            for x in data['factTransfers']
        ]
        return resp 
    #params
    ts_url = kwargs['source_params']['ts_url']
    relative_path = kwargs['source_params']['conversion_fact_transfers_path']
    auth_info = kwargs['source_params']['auth_info']
    
    mssql_dsn = kwargs['dest_params']['mssql_dsn']
    load_query_mssql = kwargs['dest_params']['queries']['update_ts_fact_transfers']
    
    entity = kwargs.get('entity','ts_fact_transfers')

    #connection
    mssql_conn = pyodbc.connect(mssql_dsn, autocommit=True)
    mssql_cursor = mssql_conn.cursor()
    
    print(f'clear data in staging for {entity}')
    mssql_cursor.execute(f"delete from stg.TS_Json where entity in ('{entity}');")
        
    token_info={'token':''}
    print('Get data from api ts')
    
    if token_info['token'] == '' or token_info['expires_at'] <= datetime.now():
                print('get new token')
                token_info =_get_token(auth_info)
    headers = {
            "Accept" : "application/json", 
            "Authorization" : "Bearer " + token_info['token']
        } 
    
    print('Get fact transfers') 
    app_url = f"{ts_url}/{relative_path}"
    response = requests.get(app_url, headers=headers, verify=auth_info['verify_ssl'])
    if response.status_code == 200:
        resp = response.json()
        resp_len = len(resp.get('factTransfers',[]))
        if resp_len>0:
            print(f'data received, len: {resp_len}')
            data = _extract_conversion_fact_transfers(resp)
            _save_to_db_mssql(data, mssql_conn, entity)
            
    else:
        raise(f'Status {response} {response.text()}') 
    
    print ('update data in db')
    mssql_cursor.execute(load_query_mssql)
    print('fact transfers are loaded')
    
    mssql_cursor.close()
    mssql_conn.close()
    
    
def load_conversion_added_facts(**kwargs):
    def _extract_added_facts(data):
        resp  = [
            {
            "addedFactId":x.get("addedFactId"),
            "conversionPeriodId":x.get("conversionPeriodId"),
            "personId":x.get("personId"),
            "projectTo":x.get("projectTo"),
            "allocationDuration":x.get("allocationDuration"),
            "createdAt":x.get("createdAt"),
            "conversionTimeAllocationId":x.get("conversionTimeAllocationId") 
            }
            for x in data['addedFacts']
        ]
        return resp 
    #params
    ts_url = kwargs['source_params']['ts_url']
    relative_path = kwargs['source_params']['conversion_added_facts_path']
    auth_info = kwargs['source_params']['auth_info']
    
    mssql_dsn = kwargs['dest_params']['mssql_dsn']
    load_query_mssql = kwargs['dest_params']['queries']['update_ts_added_facts']
    
    entity = kwargs.get('entity','ts_added_facts')

    #connection
    mssql_conn = pyodbc.connect(mssql_dsn, autocommit=True)
    mssql_cursor = mssql_conn.cursor()
    
    print(f'clear data in staging for {entity}')
    mssql_cursor.execute(f"delete from stg.TS_Json where entity in ('{entity}');")
        
    token_info={'token':''}
    print('Get data from api ts')
    
    if token_info['token'] == '' or token_info['expires_at'] <= datetime.now():
                print('get new token')
                token_info =_get_token(auth_info)
    headers = {
            "Accept" : "application/json", 
            "Authorization" : "Bearer " + token_info['token']
        } 
    
    print('Get added facts') 
    app_url = f"{ts_url}/{relative_path}"
    response = requests.get(app_url, headers=headers, verify=auth_info['verify_ssl'])
    if response.status_code == 200:
        resp = response.json()
        resp_len = len(resp.get('addedFacts',[]))
        if resp_len>0:
            print(f'data received, len: {resp_len}')
            data = _extract_added_facts(resp)
            _save_to_db_mssql(data, mssql_conn, entity)
            
    else:
        raise(f'Status {response} {response.text()}') 
    
    print ('update data in db')
    mssql_cursor.execute(load_query_mssql)
    print('fact transfers are loaded')
    
    mssql_cursor.close()
    mssql_conn.close()