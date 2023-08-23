import pyodbc

def load_plan_hours(
    params: dict, 
    last_update_date: str, 
    execution_datetime: str
) -> None:
    try:
        
        print('Loading plan hours...')
        
        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._connect_to_database import connect_to_database
        
        rms_dsn: str = get_mssql_dsn(params, 'rms')
        
        rms_conn: pyodbc.Connection = connect_to_database(rms_dsn)
        
        rms_cursor: pyodbc.Cursor = rms_conn.cursor()
        
        select_query: str = f'''
            WITH HoursBudgetHistories AS (
                SELECT 
                    N'Direct' AS [Type],
                    N'Direct on Role' AS SubType,
                    HoursRoles.Id AS HoursRoleId,
                    -1 AS HoursCompetenceId,
                    HoursProjects.ProjectId,
                    HoursLegalEntities.LegalEntityId,
                    HoursTribes.TribeId,
                    HoursRoleHistories.[Period],
                    HoursRoleHistories.Increment,
                    HoursRoles.RoleId,
                    NULL AS CompetenceId,
                    HoursRoleHistories.[Date],
                    HoursRoleHistories.[User],
                    HoursRoleHistories.Comment
                FROM [RMS.Projects].dbo.HoursLegalEntities
                    INNER JOIN [RMS.Projects].dbo.HoursTribes
                        ON HoursTribes.HoursLegalEntityId = HoursLegalEntities.Id
                    INNER JOIN [RMS.Projects].dbo.HoursProjects
                        ON HoursProjects.Id = HoursLegalEntities.HoursProjectId
                    INNER JOIN [RMS.Projects].dbo.HoursRoles
                        ON HoursRoles.HoursTribeId = HoursTribes.Id
                    INNER JOIN [RMS.Projects].dbo.HoursRoleHistories
                        ON HoursRoles.Id = HoursRoleHistories.HoursRoleId
                WHERE 1=1
                    AND HoursRoleHistories.[Date] >= '{last_update_date}'
                    AND HoursRoleHistories.[Date] < '{execution_datetime}'
                UNION ALL
                SELECT 
                    N'Direct' AS [Type],
                    N'Direct on Competence' AS SubType,
                    HoursRoles.Id,
                    HoursCompetences.Id,
                    HoursProjects.ProjectId,
                    HoursLegalEntities.LegalEntityId,
                    HoursTribes.TribeId,
                    HoursCompetenceHistories.[Period],
                    HoursCompetenceHistories.Increment,
                    HoursRoles.RoleId,
                    HoursCompetences.CompetenceId,
                    HoursCompetenceHistories.[Date],
                    HoursCompetenceHistories.[User],
                    HoursCompetenceHistories.Comment
                FROM [RMS.Projects].dbo.HoursLegalEntities
                    INNER JOIN [RMS.Projects].dbo.HoursTribes
                        ON HoursTribes.HoursLegalEntityId = HoursLegalEntities.Id
                    INNER JOIN [RMS.Projects].dbo.HoursProjects
                        ON HoursProjects.Id = HoursLegalEntities.HoursProjectId
                    INNER JOIN [RMS.Projects].dbo.HoursRoles
                        ON HoursRoles.HoursTribeId = HoursTribes.Id
                    INNER JOIN [RMS.Projects].dbo.HoursCompetences
                        ON HoursRoles.Id = HoursCompetences.HoursRoleId
                    INNER JOIN [RMS.Projects].dbo.HoursCompetenceHistories
                        ON HoursCompetenceHistories.HoursCompetenceId = HoursCompetences.Id
                WHERE 1=1
                    AND HoursCompetenceHistories.[Date] >= '{last_update_date}'
                    AND HoursCompetenceHistories.[Date] < ' {execution_datetime}'
            ),
                OUTPUT_DATA AS (
                    SELECT 
                        Projects.ProjectCode,
                        HoursBudgetHistories.LegalEntityId,
                        HoursBudgetHistories.TribeId,
                        HoursBudgetHistories.RoleId,
                        ISNULL(HoursBudgetHistories.CompetenceId, -1) AS CompetenceId,
                        CAST(HoursBudgetHistories.Period AS DATE) AS [Period],
                        HoursBudgetHistories.Type,
                        HoursBudgetHistories.SubType,
                        HoursBudgetHistories.Date AS ChangeDate,
                        Employee.Id AS [ChangedBy_EmployeeCode],
                        HoursBudgetHistories.Comment,
                        HoursBudgetHistories.Increment
                    FROM HoursBudgetHistories
                        INNER JOIN [RMS.Projects].dbo.Projects
                            ON Projects.Id = HoursBudgetHistories.ProjectId
                        LEFT JOIN [RMS.Employee].dbo.Employee
                            ON Employee.Id = HoursBudgetHistories.[User]
                    WHERE HoursBudgetHistories.Increment <> 0
            )
            SELECT 
                ProjectCode,
                LegalEntityId,
                TribeId,
                RoleId,
                CompetenceId,
                [Period],
                [Type],
                [SubType],
                ChangeDate,
                ChangedBy_EmployeeCode,
                Comment,
                Increment,
                N'RMS' AS SourceSystem
            FROM OUTPUT_DATA
        
        '''
        
        print(select_query)
        
        # Execute SQL-query
        data: list = rms_cursor.execute(select_query).fetchall()

        # Close MSSQL connection
        rms_conn.close()
        
        
        if len(data) > 0:
            dwh_dsn: str = get_mssql_dsn(params, 'dwh')
            dwh_conn: pyodbc.Connection = connect_to_database(dwh_dsn)
            dwh_cursor: pyodbc.Cursor = dwh_conn.cursor()
        
            dwh_cursor.executemany("""
                INSERT INTO [stg].[RMS_PlanHours_History] (
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
                    [SourceSystem]
                ) 
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, data)
        
            dwh_conn.commit()
            dwh_conn.close()
        
    except Exception as e:
        print('Loading plan hours. Failed.')
        print(e)
        raise e
    else:
        print('Loading plan hours. Success.')
        