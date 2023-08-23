import pandas as pd
import pyodbc


def get_data(sql_query: str, params: dict) -> pd.DataFrame:
    try:
        
        print('[Custom] Extracting data from Tabular Model...')
        
        # Import required util functions
        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._connect_to_dwh import connect_to_dwh
        
        # Get MSSQL dsn and MSSQL connection
        mssql_dsn: str = get_mssql_dsn(params)
        mssql_connection: pyodbc.Connection = connect_to_dwh(mssql_dsn)
        
        # Execute query and get data from MSSQL
        data: list = _execute_query(mssql_connection, sql_query)
        
        # Create dataframe
        dataframe: pd.DataFrame = _create_dataframe(
            data, 
            params['attribute_list'], 
            params['measure_list']
        )
        
        # Close MSSQL connection
        mssql_connection.close()
            
    except Exception as e:
        print('[Custom] Extracting data from Tabular Model. Failed')
        print(e)
        raise e
    else:
        print('[Custom] Extracting data from Tabular Model. Success')
        return dataframe

#* -------------------------------------------------------------------------- *#

def _execute_query(connection: pyodbc.Connection, query: str) -> list:
    try:
        
        print('[Custom] Executing query...')
        
        # Get MSSQL cursor
        mssql_cursor: pyodbc.Cursor = connection.cursor()
        
        # Execute query and get data from it like list
        rows: list = mssql_cursor.execute(query).fetchall()
        
    except Exception as e:
        print('[Custom] Executing query. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Executing query. Success.')
        return rows  

def _create_dataframe(data: list, attributes: list, measures: list) -> pd.DataFrame:
    try:
        
        print('[Custom] Creating dataframe...')
        
        # Concatenate attribute and measure name lists
        columns: list = attributes + measures
        
        # Create data frame with data and column names
        dataframe: pd.DataFrame = pd.DataFrame(
            [tuple(row) for row in data], 
            columns=columns
        )
        
    except Exception as e:
        print('[Custom] Creating dataframe. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Creating dataframe. Success.')
        return dataframe













   

    

    
    
    
    
    





        
        