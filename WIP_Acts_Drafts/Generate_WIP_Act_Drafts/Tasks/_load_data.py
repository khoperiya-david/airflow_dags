import pyodbc
import pandas as pd


def load_data_to_stage_table(dataframe: pd.DataFrame, params: dict) -> None:
    try:
        
        print('[Custom] Loading data to DWH...')
        
        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._connect_to_dwh import connect_to_dwh
        
        mssql_dsn: str = get_mssql_dsn(params)
        mssql_connection: pyodbc.Connection = connect_to_dwh(mssql_dsn)
        
        _clear_stg_table(mssql_connection)
        
        data: list = _transform_dataframe_to_list(dataframe)
        
        _insert_data_to_stage_table(data, mssql_connection)
        
        mssql_connection.close()
    except Exception as e:
        print('[Custom] Loading data to DWH.Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Loading data to DWH.Success.')
        
#* -------------------------------------------------------------------------- *#


def _clear_stg_table(connection: pyodbc.Connection) -> None:
    try:
        
        print('[Custom] Clearing stage table...')
        
        mssql_cursor: pyodbc.Cursor = connection.cursor()
        
        mssql_cursor.execute('TRUNCATE TABLE [stg].[WIP_Acts_Drafts]')
        mssql_cursor.commit()
        
    except Exception as e:
        print('[Custom] Clearing stage table.Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Clearing stage table.Success.') 

def _transform_dataframe_to_list(dataframe: pd.DataFrame) -> list:
    try:
        
        print('[Custom] Transfroming dataframe into list...')
        
        data: list = []
        
        for index, row in dataframe.iterrows():
            data.append(tuple([
                row.guid, 
                row.merged_y, 
                row.merged_x, 
                row.Year, 
                row.Month, 
                row.CalculationDate
            ]))
            
    except Exception as e:
        print('[Custom] Transfroming dataframe into list.Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Transfroming dataframe into list.Success.')
        return data 

def _insert_data_to_stage_table(
    data: list, 
    connection: pyodbc.Connection
) -> None:
    try:
        
        print('[Custom] Inserting data into stage table...')
        
        mssql_cursor: pyodbc.Cursor = connection.cursor()
        
        mssql_cursor.executemany("""
            INSERT INTO [stg].[WIP_Acts_Drafts] (
                ETL_LOG_ID, 
                ActId, 
                Attributes, 
                Measures, 
                YearNumber, 
                MonthNumber, 
                CalculationDate
            ) 
            VALUES (
                0x0, ?, ?, ?, ?, ?, ?
            )
        """, data)
        
        connection.commit()
        
    except Exception as e:
        print('[Custom] Inserting data into stage table.Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Inserting data into stage table.Success.')


        

