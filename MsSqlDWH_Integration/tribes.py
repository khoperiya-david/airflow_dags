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

POSTGRES_CONN_ID = "v_postgre_bi_dwh"
DWH_CONN_ID = "OLAP_Staging_DW"

default_args ={
    'on_failure_callback':failure_function
}

dag_params = {
    'dag_id': 'Tribes',
    'schedule_interval':'0 3 * * *',
    'start_date':datetime(2023, 1, 1),
    'tags':["MS SQL DWH Integration"],
    'default_args':default_args,
    'description':'Load Tribes'
}


def insert_data():   
    import pandas as pd
    from pangres import upsert
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook  
    
    source = MsSqlHook(DWH_CONN_ID) 
    source_conn = source.get_conn()
    source_cursor = source_conn.cursor()
    query="""SELECT 
                rt.TribeCode AS id
                ,rt.Name AS name
                ,rt.Description AS description
                ,e.PersonCode tribe_owner_id
                ,e.NameLocal AS tribe_owner_name
                ,rt.ClusterId AS cluster_id
            FROM 
                dbo.RMS_Tribes rt
                LEFT JOIN RMS_Person e
                    ON e.PersonCode = rt.TribeLeaderId
            WHERE rt.IS_DELETED = 0"""
    source_cursor.execute(query)
    row = source_cursor.fetchone()
    target = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_bi_dwh = create_engine(target.get_uri())
    data = []

    while row:
        rec = (row[0],row[1],row[2],row[3],row[4],row[5])
        data.append(rec)
        row = source_cursor.fetchone()

    df = pd.DataFrame(data,
                      columns=['id','name', 'description', 'tribe_owner_id', 'tribe_owner_name', 'cluster_id'])
    df.set_index(['id'], inplace=True, drop=True)
    upsert(con= pg_bi_dwh,
           df=df,
           schema='stg',
           table_name='tribes',
           if_row_exists='update',
           create_table=False)

def exec_sql(**kwargs):
    connection = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_bi_dwh = create_engine(connection.get_uri())
    pg_bi_dwh.execute(kwargs['sql'])

with DAG(**dag_params) as dag:  
                        
    truncate_stg_table = PythonOperator(
        task_id='truncate_stg_tribes',
        python_callable=exec_sql,
        op_kwargs={'sql': 'truncate table stg.tribes;'}
    )

    insert_data_stg = PythonOperator(
        task_id='insert_stg_tribes',
        python_callable=insert_data
    )

    merge_to_dwh = PythonOperator(
        task_id='merge_tribes',
        python_callable=exec_sql,
        op_kwargs={'sql': 'call stg.pr_tribes_actualization();'}
    )    


truncate_stg_table >> insert_data_stg >> merge_to_dwh