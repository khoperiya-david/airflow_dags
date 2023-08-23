import pyodbc

def define_period(params: dict) -> dict:
    try:

        print('[Custom] Getting open period...')
        
        # Import required util functions 
        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._connect_to_dwh import connect_to_dwh
        
        # Define query text
        query: str = '''     
            SELECT 
                CONVERT(
                    NVARCHAR(10), 
                    DATEADD(MONTH, 1, MAX([fps].[PERIOD_DATE]))
                ) AS [PeriodStart],
                CONVERT(
                    NVARCHAR(10), 
                    DATEADD(
                        DAY, 
                        -1, 
                        DATEADD(MONTH, 2, MAX([fps].[PERIOD_DATE]))
                    )
                ) AS [PeriodEnd],
                CONCAT(
                    YEAR(DATEADD(MONTH, 1, MAX([fps].[PERIOD_DATE]))), 
                    '-01-01'
                ) AS [Year]
            FROM [dbo].[DWH_FinancialPeriodStatus] AS [fps]
            WHERE [fps].[TS_PERIOD_IS_CLOSED] = 1
        '''
        
        # Get MSSQL DWH connection string
        conn_string: str = get_mssql_dsn(params)
        
        # Get MSSQL DWH connection
        mssql_conn: pyodbc.Connection = connect_to_dwh(conn_string)
        
        # Get query results as dictionary
        result: dict = _execute_query(mssql_conn, query)
        
    except Exception as e:
        print('[Custom] Getting open period. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Getting open period. Success.')
        return result

#* -------------------------------------------------------------------------- *#
    
    
def _execute_query(connection: pyodbc.Connection , query: str) -> dict:
    try:

        print('[Custom] Executing query...')
        
        # Get MSSQL DWH cursor
        mssql_cursor: pyodbc.Cursor = connection.cursor()
        
        # Get query resalt as row
        row: pyodbc.Row = mssql_cursor.execute(query).fetchone()
        
        # Transform result row into list
        row_list: list = [elem for elem in row]
        
        # Define result distionary 
        result: dict = {
            "date_from": row_list[0], 
            "date_to": row_list[1], 
            "year": row_list[2]
        }
        
        # Close MSSQL DWH connection
        connection.close()
        
    except Exception as e:
        print('[Custom] Executing query. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Executing query. Success.')
        return result
        