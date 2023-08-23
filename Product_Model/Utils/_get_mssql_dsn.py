def get_mssql_dsn(params: dict) -> str:
    try:
        print(f"[Custom] Getting MS SQL Server {params['DWH_environment']} dsn...")
        from airflow.hooks.base_hook import BaseHook
        if params['DWH_environment'] == 'prod':
            secrets = BaseHook.get_connection('v_mssql_olap_staging_dw')
        else:
            secrets = BaseHook.get_connection('v_mssql_olap_staging_dw_test')
        mssql_dsn: str = f"DRIVER={{ODBC Driver 18 for SQL Server}};" + \
                         f"SERVER={secrets.host};" + \
                         f"DATABASE={secrets.schema};" + \
                         f"UID={secrets.login};" + \
                         f"PWD={secrets.password};" + \
                         f"TrustServerCertificate=yes;"
    except Exception as e:
        print(f"[Custom] Getting MS SQL Server {params['DWH_environment']} dsn. Failed.")
        print(e)
        raise e
    else:
        print(f"[Custom] Getting MS SQL Server {params['DWH_environment']} dsn. Success.")
        return mssql_dsn