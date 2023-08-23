import pyodbc

def update_dwh_periods(params: dict) -> None:
    try:
        
        print(f'[Custom] Updating dwh periods...')
        
        # Import util functions
        from Utils._get_auth_token import get_auth_token
        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._connect_to_dwh import connect_to_dwh
        from Utils._get_api_url import get_api_url
        
        # Import required libraries
        import requests
        import json
        
        # Get ISSO auth token
        token_info = get_auth_token(params)
        
        # Get API url 
        api_url = get_api_url('period')
        
        # Define request headers
        headers: dict = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {token_info["token"]}'
        }
        
        # Run request and get data
        req: requests.Response = requests.get(api_url, headers=headers)
        
        # Format data as JSON
        data = req.json()
        
        # Prepare data to DWH stored procedure parameter
        data_json: str = json.dumps(
            data, 
            ensure_ascii=False
        ).replace("'", "''").encode('utf8')
        
        # Define SQL-query
        query: str = f"""
            EXECUTE [dbo].[pr_TS_Period_Actualization] '{data_json.decode()}'
        """
        
        # Get DSN string for mssql DWH
        mssql_dsn: str = get_mssql_dsn(params)
        
        # Get MSSQL DWH connection
        mssql_conn: pyodbc.Connection = connect_to_dwh(mssql_dsn)
        
        # Execure stored procedure with parameter
        _execute_query(mssql_conn, query)
        
    except Exception as e:
        print(f'[Custom] Updating dwh periods. Failed')
        print(e)
        raise e
    else:
        print(f'[Custom] Updating dwh periods. Success')
   
#* -------------------------------------------------------------------------- *#     

    
def _execute_query(connection, query: str) -> None:
    try:
        print('[Custom] Executing query...')
        
        # Get MSSQL DWH cursor
        mssql_cursor: pyodbc.Cursor = connection.cursor()
        
        # Execute query
        mssql_cursor.execute(query)
        
        # Close MSSQL DWH connection
        connection.close()
        
    except Exception as e:
        print('[Custom] Executing query. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Executing query. Success.')
        return None