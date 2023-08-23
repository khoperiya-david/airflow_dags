import pyodbc

def insert_data_to_dbo(params: dict) -> None:
    try:
        
        print('Inserting data to dbo table...')
        
        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._connect_to_database import connect_to_database
        
        dwh_dsn: str = get_mssql_dsn(params, 'dwh')
        
        dwh_conn: pyodbc.Connection = connect_to_database(dwh_dsn)
        
        dwh_cursor: pyodbc.Cursor = dwh_conn.cursor()
        
        query: str = '''
            INSERT INTO [dbo].[RMS_PlanHours_History] (
                [ProjectCode],
                [LegalEntityId],
                [TribeId],
                [RoleId],
                [CompetenceId],
                [Period],
                [Type],
                [SubType],
                [ChangeDate],
                [ChangedBy_EmployeeCode],
                [Comment],
                [Increment],
                [SourceSystem],
                [LoadDateTime]
            )
            SELECT 
                [ProjectCode],
                [LegalEntityId],
                [TribeId],
                [RoleId],
                [CompetenceId],
                [Period],
                [Type],
                [SubType],
                [ChangeDate],
                [ChangedBy_EmployeeCode],
                [Comment],
                [Increment],
                [SourceSystem],
                [LoadDateTime]
            FROM [stg].[RMS_PlanHours_History]
        '''
        
        
        dwh_cursor.execute(query)
        
        dwh_conn.commit()
        
        dwh_conn.close()
        
    except Exception as e:
        print('Inserting data to dbo table.. Failed.')
        print(e)
        raise e
    else:
        print('Inserting data to dbo table.. Success.')