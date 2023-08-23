import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from sqlalchemy.engine import create_engine
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook   
from bi_operators.email_on_failure import failure_function


log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "v_postgres_bi_dwh"
DWH_CONN_ID = "OLAP_Staging_DW"
POSTGRESQL_CONN_ID = "v_postgresql_bi_dwh"

default_args ={
    'on_failure_callback':failure_function
}

dag_params = {
    'dag_id': 'Jira_AllIs_IssuesStatus',
    'schedule_interval':'0 * * * *',
    'start_date':datetime(2023, 1, 1),
    'tags':["MS SQL DWH Integration", "PG DWH Integration", "Jira"],
    'default_args':default_args,
    'description':'Load Jira Issues Statuses From Postgre'
}

def update_data_pg():
    print('update data in postgres dwh from issues')
    source = BaseHook.get_connection(POSTGRES_CONN_ID)
    source_engine = create_engine(source.get_uri())
    query="CALL stg.pr_issue_gitlab_custom_status_actualization();"
    with source_engine.connect() as source_conn:
        source_conn.execute(query)  
    
def insert_data():  
    source = BaseHook.get_connection(POSTGRESQL_CONN_ID)
    source_engine = create_engine(source.get_uri())
    query="""
        SELECT
            ics.issue_id,
            i.issue_name AS title,
            ics.product_code_sol AS custom_sol_sowtware,
            ics.cluster_name as solclustertech
        FROM jira.issue_gitlab_custom_status ics
        join jira.issues i
            on i.issue_id = ics.issue_id"""
    
    target = MsSqlHook(DWH_CONN_ID) 
    target_conn = target.get_conn()
    target_cursor = target_conn.cursor()

    with source_engine.connect() as source_conn:
        for row in source_conn.execute(query):    
            insert_query = f"""INSERT INTO stg.Jira_AllIs_IssuesStatus (IssueId, Title, CustomSolSoftware, ClusterName)
            VALUES
            ({row[0]},'{row[1]}','{row[2]}','{row[3]}')"""
            target_cursor.execute(insert_query)
    target_conn.commit()
    target_cursor.close()
    target_conn.close()

def load_data_from_mssql_to_pg():
    
    target = BaseHook.get_connection(POSTGRESQL_CONN_ID)
    target_engine = create_engine(target.get_uri())
    
    target_engine.execute('truncate table stg.jira_issue_gitlab_custom_status;')
    
    query = """
    select ist.IssueId
     , st.Id as GitLabStatusId
     , st.Name as GitLabStatusName
    from dbo.Jira_AllIs_IssuesStatus ist 
        join dbo.Jira_StatusGitLab st 
            on ist.StatusGitLabId = st.Id"""
    source = MsSqlHook(DWH_CONN_ID) 
    source_conn = source.get_conn()
    source_cursor = source_conn.cursor()
    source_cursor.execute(query)
    data = source_cursor.fetchall()
    
    
    for row in data:
        insert_query = f"""INSERT INTO stg.jira_issue_gitlab_custom_status (issue_id, git_lab_status_id, git_lab_status_name)
            VALUES
            ({row[0]},'{row[1]}','{row[2]}')"""
        target_engine.execute(insert_query)   
        
    update_query = """
    update jira.issue_gitlab_custom_status j
    set updated_at = now() at time zone 'utc'
    , gitlab_status_id = s.git_lab_status_id
    , gitlab_status_name = s.git_lab_status_name
    from stg.jira_issue_gitlab_custom_status s
    where s.issue_id = j.issue_id
    and (
        coalesce(j.gitlab_status_name,'') != coalesce(s.git_lab_status_name,'')
    or coalesce(j.gitlab_status_id,0) != coalesce(s.git_lab_status_id,0)
    );
    """
    target_engine.execute(update_query)

def exec_sql(**kwargs):
    target = MsSqlHook(DWH_CONN_ID) 
    target_conn = target.get_conn()
    target_cursor = target_conn.cursor()
    target_cursor.execute(kwargs['sql'])
    target_conn.commit()
    target_cursor.close()
    target_conn.close()

with DAG(**dag_params) as dag:  
    update_data_pg_table = PostgresOperator(
        task_id="execute_sql_command_pg_dwh",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call stg.pr_boss_referent_integration_actualization();"      
    )          
    
    truncate_stg_table = PythonOperator(
        task_id='truncate_stg_Jira_AllIs_IssuesStatus',
        python_callable=exec_sql,
        op_kwargs={'sql': 'truncate table stg.Jira_AllIs_IssuesStatus'}
    )

    insert_data_stg = PythonOperator(
        task_id='insert_stg_Jira_AllIs_IssuesStatus',
        python_callable=insert_data
    )

    merge_to_dwh = PythonOperator(
        task_id='merge_Jira_AllIs_IssuesStatus',
        python_callable=exec_sql,
        op_kwargs={'sql': 'EXEC stg.pr_Jira_AllIs_IssuesStatus_actualization'}
    )
    from_mssql_to_pg = PythonOperator(
        task_id = 'load_data_from_mssql_to_postgres_dwh',
        python_callable=load_data_from_mssql_to_pg
    )

update_data_pg_table >> truncate_stg_table >> insert_data_stg >> merge_to_dwh >> from_mssql_to_pg