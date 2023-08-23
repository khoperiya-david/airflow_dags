import sys
# Setting up logging
import logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

def get_mssql_dsn(params: dict) -> str:
    try:
        log.info(f"[Custom] Getting MS SQL Server {params['DWH_environment']} dsn...")
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
        log.error(f"[Custom] Getting MS SQL Server {params['DWH_environment']} dsn. Failed.")
        log.error(e)
        sys.exit(1)
    else:
        log.info(f"[Custom] Getting MS SQL Server {params['DWH_environment']} dsn. Success.")
        return mssql_dsn