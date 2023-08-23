import pyodbc
import pandas as pd


def get_data(sql_query: str, params: dict) -> pd.DataFrame:
    """
    Get data from DWH and create dataframe with it.
    """
    try:
        
        print('[Custom] Extracting data from dwh...')
        
        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._connect_to_dwh import connect_to_dwh
        
        mssql_dsn: str = get_mssql_dsn(params)
        
        mssql_connection: pyodbc.Connection = connect_to_dwh(mssql_dsn)
        
        data: list = _execute_query(mssql_connection, sql_query)
        
        dataframe: pd.DataFrame = _create_dataframe(data) 
        
        mssql_connection.close()
        
    except Exception as e:
        print('[Custom] Extracting data from dwh.Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Extracting data from dwh.Success.')
        return dataframe
   
#* -------------------------------------------------------------------------- *#
   
 
def _execute_query(connection: pyodbc.Connection, query: str) -> list:
    """
    Execute given query on given connection.
    """
    try:
        
        print('[Custom] Executing query...')
        
        mssql_cursor: pyodbc.Cursor = connection.cursor()
        
        rows: list = mssql_cursor.execute(query).fetchall()

    except Exception as e:
        print('[Custom] Executing query.Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Executing query.Success.')
        return rows     
      
def _create_dataframe(data: list) -> pd.DataFrame:
    """
    Create dataframe from given list of data.
    """
    try:
        
        print('[Custom] Creating dataframe...')
        
        dataframe: pd.DataFrame = pd.DataFrame(
            [tuple(row) for row in data], 
            columns=[
                'id', 
                'ProjectId', 
                'ProjectCode', 
                'CustomerId', 
                'ContractorId', 
                'CurrenrId', 
                'ContractId', 
                'ContractName', 
                'ContractFrame', 
                'ContractCount' , 
                'Date' , 
                'StartPeriodDate', 
                'EndPeriodDate', 
                'ActStatus', 
                'ActNumber', 
                'ActType', 
                'Hyperion', 
                'TaskOebsName', 
                'CapexOpex', 
                'CostSegmentCode', 
                'ResponsibilityCenterCode', 
                'WIPCustomerCluster', 
                'Amount', 
                'SumFot', 
                'SumMargin', 
                'SumLeaders', 
                'SumOverheadCosts'
            ]
        )
        
    except Exception as e:
        print('[Custom] Creating dataframe.Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Creating dataframe.Success.')
        return dataframe
