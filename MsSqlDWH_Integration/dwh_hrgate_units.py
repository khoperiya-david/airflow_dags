import airflow
import logging

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from sqlalchemy.engine import create_engine
from contextlib import closing
from airflow.exceptions import AirflowFailException
from bi_operators.email_on_failure import failure_function


log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "v_postgresql_bi_dwh"
DWH_CONN_ID = "OLAP_Staging_DW"
default_args ={
    'on_failure_callback':failure_function
}

dag_params = {
    'dag_id': 'DWH_HRGate_Units',
    'schedule_interval':'0 4 * * *',
    'start_date':datetime(2023, 1, 1),
    'tags':["MS SQL DWH Integration", "HRGate"],  
    'default_args':default_args,
    'description':'Load DWH HRGate Units'
}


def insert_data():   
    import pandas as pd
    from pangres import upsert
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook  
    source = MsSqlHook(DWH_CONN_ID) 
    source_conn = source.get_conn()
    source_cursor = source_conn.cursor()
    query="""
    SELECT u.[UnitId] as UnitIdHrGate
     , u.[Title] as UnitNameHrGate
     , u.[Description]
     , u.[OrgCode]
     , nullif(t.ClusterId,0) as ClusterCodeRms
     , case when isnull(t.ClusterId,0)=0 then null else cl.Name end as ClusterNameRms
     , nullif(ms.TribeCode,0) as TribeCodeRms
     , case when isnull(ms.TribeCode,0) =0 then null else t.Name end as TribeNameRms
     , nullif(ms.ChapterCode,0) as ChapterCodeRms
     , case when isnull(ch.ChapterCode,0)=0 then null else ch.Name end as ChapterNameRms
FROM dbo.HRGate_Units u
    LEFT JOIN dbo.HRGate_Units_ManagementStructure ms
        ON u.UnitId = ms.UnitId
    LEFT JOIN dbo.RMS_Tribes t 
        ON ms.TribeCode = t.TribeCode
            AND t.IS_DELETED = 0  
    LEFT JOIN dbo.RMS_Clusters cl 
        ON t.ClusterId = cl.ClusterCode
            AND cl.IS_DELETED = 0
    LEFT JOIN dbo.RMS_Chapters ch 
        ON ms.ChapterCode = ch.ChapterCode
            and ch.IS_DELETED = 0;
    """
    source_cursor.execute(query)
    row = source_cursor.fetchone()
    target = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_bi_dwh = create_engine(target.get_uri())
    data = []
    while row:
        rec = (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9])
        data.append(rec)
        row = source_cursor.fetchone()

    source_cursor.close()
    source_conn.close()

    df = pd.DataFrame(data,
        columns=['id','title', 'description', 'org_code', 'cluster_id', 'cluster_name_rms', 'tribe_id', 'tribe_name_rms', 'chapter_id', 'chapter_name_rms'])
    df.set_index(['id'], inplace=True, drop=True)

    upsert(con= pg_bi_dwh,
           df=df,
           schema='stg',
           table_name='dwh_hrgate_units',
           if_row_exists='update',
           create_table=False)

def exec_sql(**kwargs):
    connection = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_bi_dwh = create_engine(connection.get_uri())
    pg_bi_dwh.execute(kwargs['sql'])

with DAG(**dag_params) as dag:  
                        
    truncate_stg_table = PythonOperator(
        task_id='truncate_stg_dwh_hrgate_units',
        python_callable=exec_sql,
        op_kwargs={'sql': 'truncate table stg.dwh_hrgate_units;'}
    )

    insert_data_stg = PythonOperator(
        task_id='insert_stg_dwh_hrgate_units',
        python_callable=insert_data
    )

    merge_to_dwh = PythonOperator(
        task_id='merge_dwh_hrgate_units',
        python_callable=exec_sql,
        op_kwargs={'sql': 'call stg.pr_dwh_hrgate_units_actualization();'}
    )

truncate_stg_table >> insert_data_stg >> merge_to_dwh