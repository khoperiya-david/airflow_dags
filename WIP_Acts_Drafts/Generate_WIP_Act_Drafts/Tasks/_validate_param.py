def validate_params(params: dict) -> None: 
    """
    Validate execution parameters before start DAG.
    
    :params : dict 
    """
    
    try:
        
        print('[Custom] Validating execution parameters...')
        
        # Import requeried libraries
        import datetime as dt
    
        # Environment validation 
        if params['DWH_environment'].lower() not in ['prod', 'test']:
            raise ValueError('DWH environment must be "prod" or "test".')
    
        # Period - year validation 
        if params['year'] < 2000 or params['year'] > dt.datetime.today().year:
            raise ValueError('Period year must be between 2000 and current year.')
        
        # Period - month validation
        if params['month'] < 1 or params['month'] > 12:
            raise ValueError('Period month must be between 1 and 12.')

        # Attribute list validation
        if len(params['attribute_list']) <= 0:
            raise ValueError('Attribute list must contains at least 1 value.')
        
        # Measure list validation
        if len(params['measure_list']) <= 0:
            raise ValueError('Measure list must contains at least 1 value.')
    
    except Exception as e:
        print('[Custom] Validating execution parameters. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Validating execution parameters. Success.')





