import pyodbc
import pandas as pd


def execute_stored_procedure(params: dict) -> None:
    try:
        print('[Custom] Loading data to DWH...')
        
        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._connect_to_dwh import connect_to_dwh
        
        mssql_dsn: str = get_mssql_dsn(params)
        
        mssql_connection: pyodbc.Connection = connect_to_dwh(mssql_dsn)
        
        _execute_sql_script(
            mssql_connection,
            params['year'],
            params['month'],
            params['force_load']
        )
        
        mssql_connection.close()
        
    except Exception as e:
        print('[Custom] Loading data to DWH.Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Loading data to DWH.Success.')
        
#* -------------------------------------------------------------------------- *#

        
def _execute_sql_script(
    connection: pyodbc.Connection, 
    year: int, 
    month: int, 
    force: int
) -> None:
    try:
        
        print('[Custom] Executing stored procedure stg.pr_WIP_Acts_Drafts_Actualization...')
        
        mssql_cursor: pyodbc.Cursor = connection.cursor()
        
        mssql_cursor.execute(f'EXECUTE [stg].[pr_WIP_Acts_Drafts_Actualization] {year}, {month}, {force}')
        
        mssql_cursor.commit()
        
    except Exception as e:
        print('[Custom] Executing stored procedure stg.pr_WIP_Acts_Drafts_Actualization.Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Executing stored procedure stg.pr_WIP_Acts_Drafts_Actualization.Success.') 
