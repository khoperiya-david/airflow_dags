def connect_to_database(conn_string: str):
    try:
        print('[Custom] Connecting to database...')
        import pyodbc 
        mssql_conn = pyodbc.connect(conn_string, autocommit=True)
    except Exception as e:
        print('[Custom] Connecting to database. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Connecting to database. Success.')
        return mssql_conn