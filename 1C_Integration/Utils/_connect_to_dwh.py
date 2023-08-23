def connect_to_dwh(conn_string: str):
    try:
        print('[Custom] Connecting to dwh...')
        import pyodbc 
        mssql_conn = pyodbc.connect(conn_string)
    except Exception as e:
        print('[Custom] Connecting to dwh. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Connecting to dwh. Success.')
        return mssql_conn