import pyodbc

def execute_stored_procedure(params: dict, procedure_name: str) -> None:
    try:
        
        print(f'[Custom] Executing stored procedure {procedure_name}...')

        # Import util functions
        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._connect_to_dwh import connect_to_dwh
        
        # Define query
        query: str = f'''
            EXECUTE {procedure_name};
        '''
        
        # Get MSSQL DWH connection string
        conn_string: str = get_mssql_dsn(params)
        
        # Get MSSQL DWH connection
        mssql_conn: pyodbc.Connection = connect_to_dwh(conn_string)
        
        # Execute query (stored procedure)
        _execute_query(mssql_conn, query)
        
    except Exception as e:
        print(f'[Custom] Executing stored procedure {procedure_name}. Failed')
        print(e)
        raise e
    else:
        print(f'[Custom] Executing stored procedure {procedure_name}. Success')  
    
#* -------------------------------------------------------------------------- *#
        
    
def _execute_query(connection, query: str) -> None:
    try:
        print('[Custom] Executing query...')
        
        # Get MSSQL DWH cursor
        mssql_cursor: pyodbc.Cursor = connection.cursor()
        
        # Execure query
        mssql_cursor.execute(query)
        
        # Close MSSQL DWH connection
        connection.close()
        
    except Exception as e:
        print('[Custom] Executing query. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Executing query. Success.')