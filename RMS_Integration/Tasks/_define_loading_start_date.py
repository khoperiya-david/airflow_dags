import pyodbc

def define_loading_start_date(params: dict) -> str:
    try:
        
        print('Defining loading start date...')
        
        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._connect_to_database import connect_to_database
        
        mssql_dsn: str = get_mssql_dsn(params, 'dwh')
        
        mssql_conn: pyodbc.Connection = connect_to_database(mssql_dsn) 
        
        query: str = '''
            SELECT CAST([il].[LastUpdateDate] AS nvarchar(63)) AS [LastUpdateDate]  
            FROM [stg].[DWH_IncrementLoading] AS [il]   
            WHERE [il].[TableName] =  '[stg].[RMS_PlanHours_History]'
        '''
        
        last_update_date: pyodbc.Row = _execute_query(mssql_conn, query)
        
        result: str = last_update_date[0][0:16] + ':00'
        
        if result == '' or params['full_load'] == 1:
            result = '1900-01-01 00:00:00'
        
    except Exception as e:
        print('Defining loading start date. Failed.')
        print(e)
        raise e  
    else:
        print('Defining loading start date. Success.')
        return result
        

        
#* -------------------------------------------------------------------------- *#
    
    
def _execute_query(connection: pyodbc.Connection, query: str) -> str:
    try:

        print('[Custom] Executing query...')

        # Get MSSQL cursor
        mssql_cursor: pyodbc.Cursor = connection.cursor()

        # Execute SQL-query
        last_update_date: str = mssql_cursor.execute(query).fetchone()

        # Close MSSQL connection
        connection.close()

    except Exception as e:
        print('[Custom] Executing query. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Executing query. Success.')
        return last_update_date