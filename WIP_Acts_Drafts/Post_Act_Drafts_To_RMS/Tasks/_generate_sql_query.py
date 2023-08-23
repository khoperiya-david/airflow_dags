def generate_sql_query(params: dict) -> str:
    """
    Generate an SQL query to the act drafts view.
    """
    try:
        
        print('[Custom] Genearting SQL-query...')
        
        year: int = params['year']
        month: int = params['month']
        
        sql_query = f'''
            SELECT
                [id],
                CAST([ProjectId] AS INT) AS [ProjectId],
                [ProjectCode],
                [CustomerId],
                [ContractorId],
                [CurrencyId] as [CurrenrId],
                [ContractId],
                [ContractName],
                [ContractFrame],
                [ContractCount],
                [Date],
                [StartPeriodDate],
                [EndPeriodDate],
                [ActStatus],
                [ActNumber],
                [ActType],
                [Hyperion],
                [TaskOebsName],
                [CapexOpex],
                [CostSegmentCode],
                [ResponsibilityCenterCode],
                [WIPCustomerCluster],
                [Amount],
                [SumFot],
                [SumMargin],
                [SumLeaders],
                [SumOverheadCosts]
            FROM [dbo].[viw_WIP_Acts_Drafts]
            WHERE 1=1
                AND [MonthNumber] = {month}
                AND [Year] = {year}
        '''
        
    except Exception as e:
        print('[Custom] Genearting SQL-query. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Genearting SQL-query. Success.')
        return sql_query

#* -------------------------------------------------------------------------- *#
