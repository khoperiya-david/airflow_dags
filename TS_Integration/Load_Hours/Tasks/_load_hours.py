import aiohttp

async def load_hours(period: dict, params: dict, hours_type: str) -> None:
    try:
        print(f'[Custom] Loading {hours_type} hours...')
        
        # Import required unit functions
        from Utils._get_auth_token import get_auth_token
        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._get_api_url import get_api_url
        
        # Import required libraries
        from datetime import datetime 
            
        # Get ISSO auth token
        token_info: dict = get_auth_token(params)
        
        # Get DSN string for mssql DWH
        mssql_dsn: str = get_mssql_dsn(params)
            
        # Get API url 
        api_url: str = get_api_url(hours_type)
        
        # Define main variables
        responses_with_data: int = 0
        loop_count: int = 0
        pool_size: int = params['pool_size']
        batch_params: dict = {
            'offset': 1,
            'limit': 10000,
            'url': api_url,
            'date_from': period['date_from'],
            'date_to': period['date_to'],
            'token': token_info['token']
        }

        # Set API Client timeout
        client_timeout: aiohttp.ClientTimeout = aiohttp.ClientTimeout(300)

        # Open API Client Session
        async with aiohttp.ClientSession(timeout=client_timeout) as session: 
        
            # Run an asynchronous request loop until an empty JSON is returned
            while (
                loop_count == 0 or 
                (loop_count == 1 and responses_with_data == 1) or 
                responses_with_data == pool_size
            ):
            
                # Get ISSO auth token if it is expired
                if token_info['expires_at'] <= datetime.now():
                    token_info: dict = get_auth_token(params)
                    batch_params['token'] = token_info['token']
            
                # Running a batch of requests asynchronous
                if loop_count == 0:
                
                    responses_with_data = await _process_batch(
                        batch_params,
                        1, 
                        mssql_dsn,
                        loop_count,
                        session,
                        hours_type
                    )
                
                    batch_params['offset'] += 1
                
                else:
                
                    responses_with_data = await _process_batch(
                        batch_params,
                        pool_size, 
                        mssql_dsn,
                        loop_count,
                        session,
                        hours_type
                    )
                
                    batch_params['offset'] += pool_size
                
                # Overwrite loop counter
                loop_count += 1
        
    except Exception as e:
        print(f'[Custom] Loading {hours_type} hours. Failed')
        print(e)
        raise e
    else:
        print(f'[Custom] Loading {hours_type} hours. Success.')
        
#* -------------------------------------------------------------------------- *#

async def _process_batch(
    batch_params: dict, 
    pool_size: int, 
    mssql_dsn: str, 
    loop_count: int,
    session: aiohttp.ClientSession,
    hours_type: str
) -> int:
    try:

        print(f'[Custom] Processing {loop_count + 1} batch...')
        
        # Import required libraries
        import asyncio
        
        # Define list of asynchronous requests
        async_tasks: list = []
        
        # Fill list of asynchronous requests
        for i in range(pool_size): 

            url: str = (
                f"{batch_params['url']}"
                f"?fromDate={batch_params['date_from']}"
                f"&toDate={batch_params['date_to']}"
                f"&queryTop={batch_params['limit']}"
                f"&querySkip={i + batch_params['offset']}"  
            )  
            
            async_tasks.append(
                asyncio.create_task(
                    _process_request(
                        session,
                        url,
                        batch_params['token'],
                        mssql_dsn,
                        hours_type
                    )
                )
            )
        
        # Get result of async tasks batch 
        batch_results: list = await asyncio.gather(*async_tasks)    
                
        # Count responses with returned data        
        responses_with_data: int = sum(batch_results)      
                       
    except Exception as e:
        print(f'[Custom] Processing {loop_count + 1} batch. Failed.')
        print(e)
        raise e
    else:
        print(f'[Custom] Processing {loop_count + 1} batch. Success.')
        return responses_with_data

async def _process_request(
    session: aiohttp.ClientSession, 
    url: str, 
    token: str, 
    mssql_dsn: str, 
    hours_type: str
) -> int:
    try:
        
        # Import required libraries
        import json
        import aioodbc

        # Define API request headers
        headers: dict = {
            'Authorization': f'Bearer {token}'
        }

        # Send request and wait response
        async with session.get(url, headers=headers) as response:
                
            # Flag for returned data. 0 by default - no data returned
            is_response_with_data: int = 0
                
            if response.status == 200:
                    
                # Get data from response as json string
                data: list = await response.json()
                    
                if len(data) > 0:
                        
                    if len(data['timeAllocations']) > 0:

                        print(f'[Custom] Returned data count: {len(data["timeAllocations"])}')    
                        
                        # Set flag for returned data. 1 - data returned
                        is_response_with_data: int = 1
                            
                        # Format json string and encode as utf8
                        data_json: str = json.dumps(
                            data['timeAllocations'], 
                            ensure_ascii=False
                        ).replace("'", "''").encode('utf8')
                            
                        # Define query for different hour types
                        if hours_type == 'normalised': 
                            query: str = f"""
                                DECLARE @val AS NVARCHAR(max)='{data_json.decode()}';
                                EXECUTE [stg].[pr_TS_Normilised_Hours_JSON_To_Stg_Table] @val;
                            """
                        else:
                            query: str = f"""
                                DECLARE @val AS NVARCHAR(max)='{data_json.decode()}';
                                EXECUTE [stg].[pr_TS_Raw_Hours_JSON_To_Stg_Table] @val;
                            """

                        # Get MSSQL connection async
                        mssql_connection: aioodbc.Connection = await aioodbc.connect(
                            dsn=mssql_dsn, 
                            autocommit=True
                        )

                        # Get MSSQL cursor async
                        mssql_cursor: aioodbc.Cursor = await mssql_connection.cursor()
                                
                        # Execute query
                        await mssql_cursor.execute(query)

                        # Close MSSQL cursor async
                        await mssql_cursor.close()
            
                        # Close MSSQL connection async
                        await mssql_connection.close()
                            
            else:
                raise Exception(response.status)
    
    except Exception as e:
        print(f'[Custom] Processing {url} request. Failed')
        print(e)
        raise e
    else:
        return is_response_with_data      
        