import pandas as pd


def transform_dataframe(dataframe: pd.DataFrame, params: dict) -> pd.DataFrame:
    try:
        
        print('[Custom] Transforming data...')
        
        # Delete rows with empty project code from dataframe
        dataframe_cleaned: pd.DataFrame = _delete_empty_project_code_rows(
            dataframe
        )
        
        # Add row number column
        dataframe_indexed: pd.DataFrame = _add_row_index_column(
            dataframe_cleaned
        )
        
        
        # Get attribute column names
        attribute_columns: list = _get_column_names(
            dataframe_indexed, 
            'attribute'
        )
        
        # Get sliced dataframe with only attribute columns
        attribute_dataframe: pd.DataFrame = _slice_dataframe_by_column_names(
            dataframe_indexed, 
            attribute_columns, 
            'attribute'
        )
        
        # Format attribute columns
        attribute_dataframe: pd.DataFrame = _format_columns(
            attribute_dataframe, 
            attribute_columns, 
            'attribute'
        )
        
        # Add uniq GUID to project donor code
        attribute_dataframe: pd.DataFrame = _add_uniq_donor_codes_guid(
            attribute_dataframe, 
            dataframe_indexed
        )
        
        # Unpivot attribute dataframe
        attribute_dataframe_unpivoted: pd.DataFrame = _unpivot_dataframe(
            attribute_dataframe, 
            'attribute'
        )
        
        
        # Get measure column names
        measure_columns: list = _get_column_names(dataframe_indexed, 'measure')
        
        # Get sliced dataframe with only measure columns
        measure_dataframe: pd.DataFrame = _slice_dataframe_by_column_names(
            dataframe_indexed, 
            measure_columns, 
            'measure'
        )
        
        # Format measure columns
        measure_dataframe: pd.DataFrame = _format_columns(
            measure_dataframe, 
            measure_columns, 
            'measure'
        )
        
        # Unpivot measure dataframe
        measure_dataframe_unpivoted: pd.DataFrame = _unpivot_dataframe(
            measure_dataframe, 
            'measure'
        )
      
      
        # Merge attribute and measure dataframe
        dataframe_transformed: pd.DataFrame = _create_ready_to_load_dataframe(
            attribute_dataframe_unpivoted, 
            measure_dataframe_unpivoted, 
            params
        )
        
    except Exception as e:
        print('[Custom] Transforming data. Failed')
        print(e)
        raise e
    else:
        print('[Custom] Transforming data. Success.')
        return dataframe_transformed

#* -------------------------------------------------------------------------- *#   


def _delete_empty_project_code_rows(dataframe: pd.DataFrame) -> pd.DataFrame:
    try:
        
        print('[Custom] Cleaning up dataframe...')
        
        # Delete rows with empty project code
        if 'Project_Donors[Project Code]' in dataframe.columns:
            dataframe_copy: pd.DataFrame = dataframe[dataframe['Projects_Donors[Project Code]'] != None].copy()
        else: 
            dataframe_copy: pd.DataFrame = dataframe.copy()
            
    except Exception as e:
        print('[Custom] Cleaning up dataframe. Failed')
        print(e)
        raise e
    else:
        print('[Custom] Cleaning up dataframe. Success.')
        return dataframe_copy

def _add_row_index_column(dataframe: pd.DataFrame) -> pd.DataFrame:
    try:
        
        print('[Custom] Adding row index column...')
        
        # Add row number column
        dataframe['row_index'] = [row_number for row_number in range(len(dataframe.index))]
        
    except Exception as e:
        print('[Custom] Adding row index column. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Adding row index column. Success.')
        return dataframe

def _get_column_names(dataframe: pd.DataFrame, column_type: str) -> list:
    try:
        
        print(f'[Custom] Getting {column_type} column names...')
        
        # Get dataframe column names
        columns: pd.Index = dataframe.columns
        
        # Get column names depending on the column type
        if column_type == 'attribute':
            columns_list: list = [column for column in columns if (column[0]  != '[' and column != 'row_index')]
            columns_list.insert(0, 'row_index')
        else:
            columns_list: list = [column for column in columns if column[0] == '[']
            columns_list.insert(0, 'row_index')
            
    except Exception as e:
        print(f'[Custom] Getting {column_type} column names. Failed.')
        print(e)
        raise e
    else:
        print(f'[Custom] Getting {column_type} column names. Success.')
        return columns_list

def _slice_dataframe_by_column_names(
    dataframe: pd.DataFrame, 
    column_names: list, 
    column_type: str
) -> pd.DataFrame:
    try:
        
        print(f'[Custom] Creating {column_type} dataframe...')
        
        # Get sliced copy of dataframe with given column names
        dataframe: pd.DataFrame = dataframe[column_names].copy()
        
    except Exception as e:
        print(f'[Custom] Creating {column_type} dataframe. Failed.')
        print(e)
        raise e
    else:
        print(f'[Custom] Creating {column_type} dataframe. Success')
        return dataframe

def _format_columns(
    dataframe: pd.DataFrame, 
    column_list: list, 
    column_type: str
) -> pd.DataFrame:
    try:
        
        print(f'[Custom] Format {column_type} columns...')
        
        # Format column depending on the column type
        if column_type == 'attribute':
            for column in column_list:
                if column != 'row_index':
                    dataframe[column] = pd.Series(dataframe[column], dtype='string').replace(r'\s+|\\n', ' ', regex=True)
                    dataframe[column] = dataframe[column].apply(lambda x: pd.to_datetime(x, dayfirst=True, errors='ignore'))
                    dataframe[column] = pd.Series(dataframe[column], dtype='string').fillna('null')
                    dataframe[column] = dataframe[column].apply(lambda x: x.replace('"', '\\"'))
        else:
            for column in column_list:
                if column != 'row_index':
                    dataframe[column] = pd.Series(dataframe[column], dtype='string')
                    dataframe[column] = dataframe[column].fillna('null').apply(lambda x: x.replace(',', '.'))
                    
    except Exception as e:
        print(f'[Custom] Format {column_type} columns. Failed.')
        print(e)
        raise e
    else:
        print(f'[Custom] Format {column_type} columns. Success.')
        return dataframe

def _add_uniq_donor_codes_guid(
    attribute_dataframe: pd.DataFrame, 
    dataframe_indexed: pd.DataFrame
) -> pd.DataFrame:
    try:
        
        print('[Custom] Adding project code guid to attribute dataframe...')
        
        # Get dataframe with uniq project donor codes
        uniq_donor_codes_dataframe = _add_uniq_guid_for_project_donor_code_column(dataframe_indexed)
        
        # Merge uniq project donor codes dataframe with attribute dataframe
        dataframe = pd.merge(
            attribute_dataframe, 
            uniq_donor_codes_dataframe, 
            on=[
                'Projects_Donors[Project Code]', 
                'Projects_Donors[Project Code]'
            ], 
            how='left'
        )
        
    except Exception as e:
        print('[Custom] Adding project code guid to attribute dataframe.Failed')
        print(e)
        raise e
    else:
        print('[Custom] Adding project code guid to attribute dataframe.Success.')
        return dataframe

def _add_uniq_guid_for_project_donor_code_column(
    dataframe: pd.DataFrame
) -> pd.DataFrame:
    try:
        
        print('[Custom] Adding project donor code guid...')
        
        # Import required library
        import uuid
        
        # Get uniq project donor codes
        uniq_donor_codes = dataframe['Projects_Donors[Project Code]'].unique()
        
        # Create dataframe with uniq project donor codes
        dataframe_unique_donor_codes = pd.DataFrame(
            uniq_donor_codes, 
            columns=['Projects_Donors[Project Code]']
        )
        
        # Add GUID column
        dataframe_unique_donor_codes['guid'] = [uuid.uuid4() for _ in range(len(dataframe_unique_donor_codes.index))] 
        
    except Exception as e:
        print('[Custom] Adding project donor code guid.Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Adding project donor code guid.Success.')
        return dataframe_unique_donor_codes

def _unpivot_dataframe(dataframe: pd.DataFrame, column_type: str) -> pd.DataFrame:
    try:
        
        print(f'[Custom] Unpivoting {column_type} dataframe...')
        
        # Unpivot dataframe depending on the column type
        if column_type == 'attribute':
            melted_dataframe = dataframe.melt(id_vars=['row_index', 'guid'])
            melted_dataframe['merged'] = melted_dataframe.agg(lambda x: f'{{"Attribute": "{x["variable"]}", "value": "{x["value"]}"}}', axis=1)
            unpivoted_dataframe = melted_dataframe[['row_index', 'guid', 'merged']].copy()
            unpivoted_dataframe = unpivoted_dataframe.groupby(['row_index', 'guid'])['merged'].apply(lambda x: ', '.join(x)).reset_index()
            unpivoted_dataframe['guid'] = unpivoted_dataframe['guid'].astype(str)
        else:
            melted_dataframe = dataframe.melt(id_vars='row_index')
            melted_dataframe['merged'] = melted_dataframe.agg(lambda x: f'{{"Measure": "{x["variable"]}", "value": {x["value"]}}}', axis=1)
            unpivoted_dataframe = melted_dataframe[['row_index', 'merged']].copy()
            unpivoted_dataframe = unpivoted_dataframe.groupby('row_index')['merged'].apply(lambda x: ', '.join(x)).reset_index()
        
        # Convert json to list of json object    
        unpivoted_dataframe['merged'] = '[' + unpivoted_dataframe['merged'] + ']'
        
        # Convert row index to string
        unpivoted_dataframe['row_index'] = unpivoted_dataframe['row_index'].astype(str)
        
    except Exception as e:
        print(f'[Custom] Unpivoting {column_type} dataframe.Failed')
        print(e)
        raise e
    else:
        print(f'[Custom] Unpivoting {column_type} dataframe.Success')
        return unpivoted_dataframe

def _create_ready_to_load_dataframe(
    attribute_dataframe_unpivoted: pd.DataFrame, 
    measure_dataframe_unpivoted: pd.DataFrame, 
    params: dict
) -> pd.DataFrame:
    try:
        
        print('[Custom] Merging attribute and mesure dataframes...')
        
        # Merge attribute and measure dataframes
        dataframe = pd.merge(
            measure_dataframe_unpivoted, 
            attribute_dataframe_unpivoted, 
            on=['row_index', 'row_index'], 
            how='inner'
        )
        
        # Add custom columns
        dataframe['Year'] = params['year'] 
        dataframe['Month'] = params['month']
        dataframe['CalculationDate'] = params['calculation_date']
        
    except Exception as e:
        print('[Custom] Merging attribute and mesure dataframes.Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Merging attribute and mesure dataframes.Success.')
        return dataframe
    