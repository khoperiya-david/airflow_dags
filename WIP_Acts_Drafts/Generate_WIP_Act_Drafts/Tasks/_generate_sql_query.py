def generate_sql_query(dax_query: str) -> str: 
    try:
        
        print('[Custom] Generating sql-query...')
        
        # Generate sql-query for linked server
        sql_query: str = f'''
            DECLARE @query as nvarchar(max);
            
            SET @query = '
                SELECT * 
                FROM OPENQUERY(PMO_TAB, '' {dax_query} '')
            ';
            
            EXECUTE sp_executeSQL @query;
        '''
        
    except Exception as e:
        print('[Custom] Generating sql-query. Failed.')
        print(e)  
        raise e
    else:
        print('[Custom] Generating sql-query. Success.')
        return sql_query