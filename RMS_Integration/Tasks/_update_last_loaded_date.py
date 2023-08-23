import pyodbc

def update_last_loaded_date(params: dict, execution_datetime: str) -> None:
    try:
        
        print('Updating last loaded date...')
        
        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._connect_to_database import connect_to_database
        
        dwh_dsn: str = get_mssql_dsn(params, 'dwh')
        
        dwh_conn: pyodbc.Connection = connect_to_database(dwh_dsn)
        
        dwh_cursor: pyodbc.Cursor = dwh_conn.cursor()
        
        query: str = f'''
            UPDATE [stg].[DWH_IncrementLoading]  
            SET LastUpdateDate = '{execution_datetime}' 
            WHERE TableName = '[stg].[RMS_PlanHours_History]'
        '''

        dwh_cursor.execute(query)
        
        dwh_conn.commit()
        
        dwh_conn.close()
        
    except Exception as e:
        print('Updating last loaded date. Failed.')
        print(e)
        raise e
    else:
        print('Updating last loaded date. Success.')