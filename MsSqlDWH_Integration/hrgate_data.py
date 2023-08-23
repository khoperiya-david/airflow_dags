import airflow
import logging

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from sqlalchemy.engine import create_engine
from contextlib import closing
from airflow.exceptions import AirflowFailException
from bi_operators.email_on_failure import failure_function


log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "v_postgres_bi_dwh"
POSTGRESQL_CONN_ID = "v_postgresql_bi_dwh"
DWH_CONN_ID = "OLAP_Staging_DW"

default_args ={
    'on_failure_callback':failure_function
}

dag_params = {
    'dag_id': 'HRGate_Persons_Data',
    'schedule_interval':'0 2 * * *',
    'start_date':datetime(2023, 1, 1),
    'tags':["MS SQL DWH Integration", "HRGate"],
    'default_args':default_args,
    'description':'Load HRGate Persons Data'
}


def insert_data():  
    import pandas as pd
    from pangres import upsert
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook    
    source = MsSqlHook(DWH_CONN_ID) 
    source_conn = source.get_conn()
    source_cursor = source_conn.cursor()
    query="""SELECT DISTINCT
	            [HRGate_Persons].PersonId hrgate_person_id,
	            [HRGate_Assignments].AssignmentId hrgate_assignment_id,
	            [HRGate_AssignmentsPersonsMapping].Login login,
	            CONVERT(varchar,[HRGate_Assignments].HireDate,121) assignment_hire_date,
	            [RMS_Roles].Name role,
	            [StaffUnits].PositionName position,
	            [HRGate_AssignmentsPersonsMapping].FIO fio,
	            [StaffUnits].BusinessUnitCode business_unit,
	            [RMS_Clusters].Name cluster,
	            [RMS_Tribes].Name tribe,
	            ISNULL([RMS_LegalEntities].LegalEntityName,[HRGate_Organizations].UnitTitle) legal_entity
            FROM 
	            [OLAP_Staging_DW].[dbo].[DimEmployee]
	            INNER JOIN [OLAP_Staging_DW].[dbo].[HRGate_AssignmentsPersonsMapping] ON [DimEmployee].EmployeeCode=[HRGate_AssignmentsPersonsMapping].PersonCode
	            INNER JOIN [OLAP_Staging_DW].[dbo].[PersonMapping] ON [PersonMapping].PersonCode=[DimEmployee].EmployeeCode
	            INNER JOIN [OLAP_Staging_DW].[dbo].[HRGate_Persons] ON [HRGate_Persons].PersonId=[PersonMapping].SourceSystemPersonId
	            INNER JOIN [OLAP_Staging_DW].[dbo].[HRGate_Assignments] ON [HRGate_AssignmentsPersonsMapping].AssignmentId = [HRGate_Assignments].AssignmentId 
	            INNER JOIN [OLAP_Staging_DW].[dbo].[HRGate_Employees] ON [HRGate_AssignmentsPersonsMapping].EmployeeId=[HRGate_Employees].EmployeeId
                INNER JOIN [OLAP_Staging_DW].[dbo].[HRGate_Organizations] ON [HRGate_Organizations].OrganizationId=[HRGate_Employees].OrganizationId
	            LEFT OUTER JOIN [OLAP_Staging_DW].[dbo].[HRGate_StaffUnits] ON [HRGate_Assignments].StaffUnitId=[HRGate_StaffUnits].StaffUnitId
	            LEFT OUTER JOIN [OLAP_Staging_DW].[core].[StaffUnits] ON CAST([HRGate_Assignments].StaffUnitId AS nvarchar(254))=[StaffUnits].SourceSystemStaffUnitId
	            LEFT OUTER JOIN [OLAP_Staging_DW].[dbo].[RMS_Clusters] ON [StaffUnits].ClusterCode=[RMS_Clusters].ClusterCode
	            LEFT OUTER JOIN [OLAP_Staging_DW].[dbo].[RMS_Tribes] ON [StaffUnits].TribeCode=[RMS_Tribes].TribeCode
	            LEFT OUTER JOIN [OLAP_Staging_DW].[dbo].[RMS_Roles] ON [StaffUnits].RoleId=[RMS_Roles].Code
	            LEFT OUTER JOIN [OLAP_Staging_DW].[dbo].[RMS_LegalEntities] ON [StaffUnits].LegalEntityCode=[RMS_LegalEntities].LegalEntityCode
            WHERE 
	            ([HRGate_AssignmentsPersonsMapping].FireDate>=getdate() OR [HRGate_AssignmentsPersonsMapping].FireDate IS NULL OR [HRGate_AssignmentsPersonsMapping].FireDate='1900-01-01')
	            AND ([HRGate_AssignmentsPersonsMapping].HireDate <= getdate() OR [HRGate_AssignmentsPersonsMapping].HireDate IS NULL OR [HRGate_AssignmentsPersonsMapping].HireDate='1900-01-01')"""
    source_cursor.execute(query)
    row = source_cursor.fetchone()
    target = BaseHook.get_connection(POSTGRESQL_CONN_ID)
    pg_bi_dwh = create_engine(target.get_uri())
    data = []

    while row:
        rec = (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10])
        data.append(rec)
        row = source_cursor.fetchone()

    df = pd.DataFrame(data,
                      columns=['hrgate_person_id', 'hrgate_assignment_id', 'login', 'assignment_hire_date', 'role', 'position', 'fio', 'business_unit', 'cluster', 'tribe', 'legal_entity'])
    df.set_index(['hrgate_person_id', 'hrgate_assignment_id'], inplace=True, drop=True)
    upsert(con= pg_bi_dwh,
           df=df,
           schema='stg',
           table_name='hrgate_data',
           if_row_exists='update',
           create_table=False)

def exec_sql(**kwargs):
    connection = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_bi_dwh = create_engine(connection.get_uri())
    pg_bi_dwh.execute(kwargs['sql'])

with DAG(**dag_params) as dag:  

    delete_stg_table = PostgresOperator(
        task_id='delete_stg_hrgate_data',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql = "delete from stg.hrgate_data;"
    )

    insert_data_stg = PythonOperator(
        task_id='insert_stg_hrgate_data',
        python_callable=insert_data
    )

    merge_to_dwh = PostgresOperator(
        task_id="merge_hrgate_data",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call stg.pr_hrgate_data_actualization();"
    )

delete_stg_table >> insert_data_stg >> merge_to_dwh