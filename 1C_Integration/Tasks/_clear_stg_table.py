import pyodbc

def clear_stg_table(params: dict, table_name: str) -> None:
    try:

        print(f'[Custom] Clearing table {table_name}...')
        
        # Import required util functions 
        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._connect_to_dwh import connect_to_dwh
        
        # Define query text
        query: str = f'''
            TRUNCATE TABLE {table_name};
        '''
        
        # Get MSSQL DWH connection string
        conn_string: str = get_mssql_dsn(params)
        
        # Get MSSQL DWH connection
        mssql_conn: pyodbc.Connection = connect_to_dwh(conn_string)
        
        # Execute query
        _execute_query(mssql_conn, query)
        
    except Exception as e:
        print(f'[Custom] Clearing table {table_name}. Failed.')
        print(e)
        raise e
    else:
        print(f'[Custom] Clearing table {table_name}. Success.')
        
#* -------------------------------------------------------------------------- *#
    
    
def _execute_query(connection: pyodbc.Connection, query: str) -> None:
    try:

        print('[Custom] Executing query...')

        # Get MSSQL cursor
        mssql_cursor: pyodbc.Cursor = connection.cursor()

        # Execute SQL-query
        mssql_cursor.execute(query)

        # Close MSSQL connection
        connection.commit()
        connection.close()

    except Exception as e:
        print('[Custom] Executing query. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Executing query. Success.')