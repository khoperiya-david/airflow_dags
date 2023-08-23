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

async def _save_to_db_pg_async(data, conn, entity):
    # data - json
    try:
        if len(data)>0:
            async with conn.cursor() as cur:
                ecoded_data = base64.b64encode(json.dumps(data).encode("utf8")).decode("utf8")
                query = f"INSERT INTO stg.sol(data, entity_name) VALUES (convert_from(decode('{ecoded_data}', 'base64'),'UTF-8')::jsonb, '{entity}')"
                # print(query)
                await cur.execute(query)
                await cur.commit()
    except Exception as e:
        print(f'Exception: {e}. Cannot save data to db.')
        raise e

async def _save_to_mssql_async (data, conn, entity):
    async def _compress_data(data):
        templ = '0x1F8B0800000000000400'
        compressed_value = gzip.compress(bytes(json.dumps(data), 'utf-16LE'))
        out = templ + compressed_value.hex()[20:]
        return out
    
    # data - json
    try:
        if len(data)>0:
            connx = await aioodbc.connect(dsn=conn, autocommit=True)
            cur =await connx.cursor() 
            binary = await _compress_data(data)
            query = f"""
            set NOCOUNT on;
            declare @val AS varbinary(max) = {binary}
            DECLARE @JsonValue nvarchar(max) = CONVERT (nvarchar(MAX), DECOMPRESS (@val))
            INSERT INTO [stg].[SOL_Json]
            (
                Entity,
                JsonText
            )
            SELECT '{entity}', @JsonValue
            """
            await cur.execute(query)
            await cur.close()
            await connx.close()


    except Exception as e:
        print(f'Exception: {e}. Cannot save data to db.')
        print(query)
        raise e

def _save_to_db_pg(data, conn, entity):
    # data - json
    try:
        if len(data)>0:
            with conn.cursor() as cur:
                ecoded_data = base64.b64encode(json.dumps(data).encode("utf8")).decode("utf8")
                query = f"INSERT INTO stg.sol(data, entity_name) VALUES (convert_from(decode('{ecoded_data}', 'base64'),'UTF-8')::jsonb, '{entity}')"
                # print(query)
                cur.execute(query)
                cur.commit()
    except Exception as e:
        print(f'Exception: {e}. Cannot save data to db.')
        raise e
    
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
    INSERT INTO [stg].[SOL_Json]
    (
        Entity,
        JsonText
    )
    SELECT '{entity}', @JsonValue
    """

    with conn.cursor() as cur:
        cur.execute(sql_query)
        cur.commit()

def load_consumptions(**kwargs):
    
    def _extract_consumptions(data):
        resp  = [
            {
              'id':x.get("id")
            , 'type':x.get("type")
            , 'consumer_product_id':x.get("consumerProductId")
            , 'food_product_module_object_id':x.get("foodProductModuleObjectId")
            , 'link_type':x.get("linkType")
            , 'link_weight':x.get("linkWeight")
            , 'integration_stage':x.get("integrationStage")
            , 'confirmed_in_obs':x.get("confirmedInOBS")
            , 'creation_date':x.get("creationDate")
            , 'last_change_date':x.get("lastChangeDate")
            , 'status':x.get("status")
            , 'comment':x.get("comment")
            }
            for x in data
        ]
        return resp 
    #params
    sol_url = kwargs['source_params']['sol_url']
    relative_path = kwargs['source_params']['sol_consumptions_path']
    auth_info = kwargs['source_params']['auth_info']
    
    mssql_dsn = kwargs['dest_params']['mssql_dsn']
    load_query_mssql = kwargs['dest_params']['queries']['update_sol_product_consumptions_mssql']
    
    entity = kwargs.get('entity','consumptions')

    #connection
    mssql_conn = pyodbc.connect(mssql_dsn, autocommit=True)
    mssql_cursor = mssql_conn.cursor()
    
    print('clear data in staging')
    mssql_cursor.execute(f"delete from stg.SOL_Json where entity in ('{entity}');")
        
    token_info={'token':''}
    print('Get data from api sol')
    
    if token_info['token'] == '' or token_info['expires_at'] <= datetime.now():
                print('get new token')
                token_info =_get_token(auth_info)
    headers = {
            "Accept" : "application/json", 
            "Authorization" : "Bearer " + token_info['token']
        } 
    
    print('Get Consumptions') 
    app_url = f"{sol_url}/{relative_path}?LinkTypes=Realization"
    response = requests.get(app_url, headers=headers, verify=auth_info['verify_ssl'])
    if response.status_code == 200:
        resp = response.json()
        resp_len = len(resp)
        if resp_len>0:
            print(f'data received, len: {resp_len}')
            data = _extract_consumptions(resp)
            _save_to_db_mssql(data, mssql_conn, entity)
            
    else:
        raise(f'Status {response} {response.text()}') 
    
    print ('update data in db')
    mssql_cursor.execute(load_query_mssql)
    print('Consumptions are loaded')
    mssql_cursor.close()
    mssql_conn.close()

def load_products(**kwargs):
    def _extract_products (data,sol_type, sol_url):
        resp = [
            {
                "id":product.get('id')
              , "objectId":product.get('objectId')
              , "parentId":product.get('parentId')
              , "code":product.get('code')
              , "name":product.get('name')
              , "rmsContour":product.get('rmsContour',{}).get('unitId')
              , "techTribe":product.get('techTribe',{}).get('unitId')
              , "owner":product.get('ownerDetails',{}).get('login')
              , "ownerName":product.get("ownerDetails",{}).get('userName')
              , "techOwner":product.get('techOwnerDetails',{}).get('login')
              , "techOwnerName":product.get("techOwnerDetails",{}).get('userName')
              , "status":product.get('status')
              , "description":product.get('description','')[:4000]
              , "shortDescription":product.get('shortDescription')
              , "itObjectStatus":product.get('itObjectStatus')
              , "sol_type":sol_type
              , 'sol_url':sol_url
            }
            for product in data
        ]
        return resp

    def _extract_classifiers (data):
        resp = [
            {
            "id":clsf.get('entityId')
            , "clf_id": clsf.get('classifier',{}).get('id')
            , "classifier_value_name":clsf.get('classifierValue',{}).get('name')
            }
            for clsf in data  if clsf.get('classifier',{}).get('id') in [8,9,27,28]
        ]
        return resp   
    
    # params
    entity = 'product'
    entity_classifiers = 'classifiers'
    
    sol_url = kwargs['source_params']['sol_url']
    relative_path = kwargs['source_params']['relative_path']
    sol_classifiers_path = kwargs['source_params']['sol_classifiers_path']
    auth_info = kwargs['source_params']['auth_info']
    types = kwargs['source_params']['types']
    
    pg_dsn = kwargs['dest_params']['pg_dsn']
    mssql_dsn = kwargs['dest_params']['mssql_dsn']
    load_query_pgsql = kwargs['dest_params']['queries']['update_sol_products_pgsql']
    load_query_mssql = kwargs['dest_params']['queries']['update_sol_products_mssql']

    #connections
    pg_conn = pyodbc.connect(pg_dsn, autocommit=True)
    pg_cursor = pg_conn.cursor()
    
    mssql_conn = pyodbc.connect(mssql_dsn, autocommit=True)
    mssql_cursor = mssql_conn.cursor()
    
    print('clear data in staging')
    pg_cursor.execute(f"delete from stg.sol where entity_name in ('{entity}','{entity_classifiers}');")
    mssql_cursor.execute(f"delete from stg.SOL_Json where entity in ('{entity}','{entity_classifiers}');")
        
    token_info={'token':''}
    print('Get data from api sol')
    
    if token_info['token'] == '' or token_info['expires_at'] <= datetime.now():
                print('get new token')
                token_info =_get_token(auth_info)
    headers = {
            "Accept" : "application/json", 
            "Authorization" : "Bearer " + token_info['token']
        } 
    print('Get sol classifiers')    
    app_url = f"{sol_url}/{sol_classifiers_path}?entityType=1"
    response = requests.get(app_url, headers=headers, verify=auth_info['verify_ssl'])
    if response.status_code == 200:
        resp = response.json()
        resp_len = len(resp)
        if resp_len>0:
            print(f'data received, len: {resp_len}')
            data = _extract_classifiers(resp)
            _save_to_db_pg(data, pg_conn, entity_classifiers)
            _save_to_db_mssql(data, mssql_conn, entity_classifiers)
            
    else:
        raise(f'Status {response} {response.text()}') 
    
    print('Get sol modules')
    for sol_type in types:
        print(f'load {sol_type}')
        if token_info['token'] == '' or token_info['expires_at'] <= datetime.now():
                print('get new token')
                token_info =_get_token(auth_info)
        headers = {
            "Accept" : "application/json", 
            "Authorization" : "Bearer " + token_info['token']
        } 
           
        app_url = f"{sol_url}/{relative_path}?includeDeleted=True&type={sol_type}"
        response = requests.get(app_url, headers=headers, verify=auth_info['verify_ssl'])
        if response.status_code == 200:
            resp = response.json()
            resp_len = len(resp)
            if resp_len>0:
                print(f'data received, len: {resp_len}')
                data = _extract_products(resp,sol_type, sol_url)
                _save_to_db_pg(data, pg_conn, entity)
                _save_to_db_mssql(data, mssql_conn, entity)
                
        else:
            raise(f'Status {response} {response.text()}') 

    print('execute update queries')
    
    pg_cursor.execute(load_query_pgsql)
    mssql_cursor.execute(load_query_mssql)
    
    pg_cursor.close()
    pg_conn.close()
    
    mssql_cursor.close()
    mssql_conn.close()
    
    print('Modules are loaded')
    
def load_ims_systems(**kwargs):
    #params
    entity = 'ims_systems'
    pool_size = kwargs.get('source_params',{}).get('pool_size',5)
    relative_path = kwargs['source_params']['relative_path']
    sol_url = kwargs['source_params']['sol_url']
    auth_info = kwargs['source_params']['auth_info']
    
    pg_dsn = kwargs['dest_params']['pg_dsn']
    mssql_dsn = kwargs['dest_params']['mssql_dsn']
    load_query_pgsql = kwargs['dest_params']['queries']['update_sol_products_pgsql']
    load_query_mssql = kwargs['dest_params']['queries']['update_ims_systems_mssql']
    
    async def _load_product_ims_systems(session, url, headers, mssql_conn, pg_conn, entity):
        data = []
        err_urls = []
        async with session.get(url=url['url'],headers = headers) as response:
            if response.status in (500, 429):
                err_urls.append({"url":url['url'], "err_type":response.status})
            if response.status == 200:
                resp = await response.json()
                if len(resp) > 0:
                    data = [
                        {
                          'object_id':x.get("objectId")
                        , 'system_id':x.get("systemId")
                        , 'name':x.get("name")
                        , 'class_name':x.get("className")
                        , 'owner_login':x.get("owner",{}).get('login')
                        , 'owner_name':x.get("owner",{}).get('userName')
                        , 'status':x.get("status")
                        , 'real_servers':x.get("realServers")
                        , 'virtual_servers':x.get("virtualServers")
                        , 'person_login':x.get("person",{}).get('login')
                        , 'person_name':x.get("person",{}).get('userName')
                        , 'url':x.get("url")
                        , 'is_test':x.get('isTest')
                    } for x in resp]
            else:
                raise Exception(response.status)
        if len(data)>0:
            await _save_to_mssql_async(data, mssql_conn, entity)
            await _save_to_db_pg_async(data, pg_conn, entity)
        return err_urls
                
    async def _load_ims_systems():
        #connections
        pg_connection = await aioodbc.connect(dsn=pg_dsn, autocommit=True)
        pg_cursor = await pg_connection.cursor()
        
        mssql_connection = await aioodbc.connect(dsn=mssql_dsn, autocommit=True)
        mssql_cursor = await mssql_connection.cursor()
        
        timeout = aiohttp.ClientTimeout(300)
        async with aiohttp.ClientSession(timeout=timeout) as session:          
                        
            print("get list of products from db")
            await mssql_cursor.execute("select ObjectId from dbo.SOL_Products")
            rows = await mssql_cursor.fetchall()
            
            print(f"Clear stg table for {entity}")
            await pg_cursor.execute(f"delete from stg.sol where entity_name in ('{entity}')")
            await mssql_cursor.execute(f"delete from stg.SOL_Json where entity in ('{entity}')")
            
            print(f'Modules will be processed: {len(rows)}')
            sol_product_ids = [row[0] for row in rows] 
            
            batch_count = ceil(len(sol_product_ids) / pool_size)
            sol_product_ids_batched = _divide_chunks(sol_product_ids, batch_count)
            
            token_info={'token':''}
            issues = []
            i:int = 0
            for batch in sol_product_ids_batched:
                if token_info['token'] == '' or token_info['expires_at'] <= datetime.now():
                    print('get new token')
                    token_info =_get_token(auth_info)
                headers = {
                    "Accept" : "application/json", 
                    "Authorization" : "Bearer " + token_info['token']
                } 
                urls = [{'url':f'{sol_url}/{relative_path}/{sol_product_id}/IMSSystemsInfo'} for sol_product_id in batch]
                if i%5 ==0:
                    print(f'batch {i}/{len(sol_product_ids_batched)} started; bad_urls count {len(list(itertools.chain.from_iterable(issues)))}')
                i += 1
                tasks = []
                for url in urls:
                    task = asyncio.create_task(_load_product_ims_systems(session=session, url=url, headers=headers, mssql_conn=mssql_dsn, pg_conn=pg_connection, entity = entity))
                    tasks.append(task)
                issues += await asyncio.gather(*tasks)
                
            errors_cnt = len(list(itertools.chain.from_iterable(issues)))
            if errors_cnt>0:
                print(f'Errors count: {list(itertools.chain.from_iterable(issues))}')
                raise('Error while loading ims systems')
            
            print('Update data in db')
            await pg_cursor.execute(load_query_pgsql)
            await mssql_cursor.execute(load_query_mssql)
            
            await pg_cursor.close()
            await pg_connection.close()
            
            await mssql_cursor.close()
            await mssql_connection.close()
            
            print('Ims systems are loaded')
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_load_ims_systems())
    