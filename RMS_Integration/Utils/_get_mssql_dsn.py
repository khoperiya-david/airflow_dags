
def get_mssql_dsn(params: dict, type: str) -> str:
    
    try:
        print(f"[Custom] Getting MS SQL Server {type} dsn...")
        
        from airflow.hooks.base_hook import BaseHook
        
        if type == 'dwh':
            if params['DWH_environment'] == 'prod':
                secrets = BaseHook.get_connection('v_mssql_olap_staging_dw')
            else:
                secrets = BaseHook.get_connection('v_mssql_olap_staging_dw_test')
        else:
            if params['RMS_environment'] == 'prod':
                secrets = BaseHook.get_connection('v_mssql_rms_projects')
            else:
                secrets = BaseHook.get_connection('v_mssql_rms_projects')
                
        mssql_dsn: str = f"DRIVER={{ODBC Driver 18 for SQL Server}};" + \
                         f"SERVER={secrets.host};" + \
                         f"DATABASE={secrets.schema};" + \
                         f"UID={secrets.login};" + \
                         f"PWD={secrets.password};" + \
                         f"TrustServerCertificate=yes;"
    except Exception as e:
        print(f"[Custom] Getting MS SQL Server {type} dsn. Failed.")
        print(e)
        raise e
    else:
        print(f"[Custom] Getting MS SQL Server {type} dsn. Success.")
        return mssql_dsn