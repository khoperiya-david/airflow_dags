def generate_dax_query(params: dict) -> str:
    try:
        
        print('[Custom] Generating DAX-query...')
        
        # Generate define part
        dax_define: str = _generate_dax_define(
            params['year'], 
            params['month']
        )
        
        # Generate evaluate part
        dax_evaluate: str = _generate_dax_evaluate(
            params['attribute_list'], 
            params['measure_list']
        )
        
        # Concatenate define and evaluate part of dax query
        dax_query: str = dax_define + dax_evaluate
        
    except Exception as e:
        print('[Custom] Generating DAX-query. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Generating DAX-query. Success.')
        return dax_query

#* -------------------------------------------------------------------------- *#


def _generate_dax_define(year: int, month: int) -> str:
    try:
        
        print('[Custom] Generating DAX-query - define part...')
        
        # Generate define part
        dax_define = f'''
            DEFINE
            VAR _DateFilter = 
                    CALCULATETABLE (  
                        VALUES ( Dates[Date] ), 
                        KEEPFILTERS ( Dates[Year] = {year} ), 
                        KEEPFILTERS ( Dates[Month Number] = {month} )
                    )
            VAR _ProjectFilter =
                    CALCULATETABLE (
                        VALUES ( Projects[Project Code] ),
                        FILTER (
                            Projects_Donors,
                            Projects_Donors[Project Status]= "Realization" &&
                            NOT ISBLANK(Projects_Donors[WIPFrameworkId]) &&
                            Projects_Donors[WIPActsProjectFilter] &&
                            Projects_Donors[Customer_Code] = 3
                        )
                    )
            VAR _ProjectsMeasureFilter =
                    CALCULATETABLE (
                        VALUES ( Projects_Donors[Project Code] ),
                        FILTER (
                            VALUES ( Projects_Donors[Project Code] ),
                            CALCULATE ( [G6InQuarterMTSD_Donors], _DateFilter ) <> 1 &&
                            CALCULATE ( [Формула AM C&M or detailed до даты] ) > 0 
                        )
                    )
        '''
        
    except Exception as e:
        print('[Custom] Generating DAX-query - define part. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Generating DAX-query - define part. Success.')
        return dax_define

def _generate_dax_evaluate(attribute_list: list, measure_list: list) -> str:
    try:
        
        print('[Custom] Generate DAX-query - evaluate part...')
        
        # Convert attribute and measure lists to strings with comma separator
        attributes: str = _convert_attribute_list_to_string(attribute_list)
        measures: str = _convert_measure_list_to_string(measure_list)
        
        # Generate evaluate part
        dax_evaluate: str = f'''
            EVALUATE
                SUMMARIZECOLUMNS (
                    {attributes},
                    _DateFilter,
                    _ProjectFilter,
                    _ProjectsMeasureFilter,
                    {measures}
                )
        '''  
    except Exception as e:  
        print('[Custom] Generate DAX-query - evaluate part. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Generate DAX-query - evaluate part. Success.')
        return dax_evaluate
    
def _convert_attribute_list_to_string(attribute_list: list) -> str:
    try:
        
        print('[Custom] Converting attribute list to string...')

        # Define empty attribute string
        attribute_string: str = ''
        
        # Count attribute list elements
        attr_count: int = len(attribute_list)
        
        # Concatenate elements from attribute list into string
        for attr_number in range(attr_count):
            # concatenate last element without comma
            if attr_number == attr_count - 1:
                attribute_string += str(attribute_list[attr_number])
            else:
                attribute_string += str(attribute_list[attr_number]) + ',\n'
        
    except Exception as e:
        print('[Custom] Converting attribute list to string. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Converting attribute list to string. Success.')
        return attribute_string
    
def _convert_measure_list_to_string(measure_list: list) -> str:
    try:
        
        print('[Custom] Converting measure list to string...')

        # Define empty measure string
        measure_string: str = ''
        
        # Count measure list elements
        measure_count: int = len(measure_list)
        
        # Concatenate elements from measure list into string
        for measure_number in range(measure_count):
            # concatenate last element without comma
            if measure_number == measure_count - 1:
                measure_string += '"' + measure_list[measure_number] + '",\n' + measure_list[measure_number] + '\n'
            else:
                measure_string += '"' + measure_list[measure_number] + '",\n' + measure_list[measure_number] + ',\n'
        
    except Exception as e:
        print('[Custom] Converting measure list to string. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Converting measure list to string. Success.')
        return measure_string











