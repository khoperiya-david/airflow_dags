def get_now_datetime_utc() -> str:
    try:
        
        print('Getting now datetime utc...')
        
        import datetime
        
        execution_datetime: str = datetime.datetime.now(
            datetime.timezone.utc
        ).strftime('%Y-%m-%d %H:%M:%S.%f')
        
        result: str = execution_datetime[0: 16] + ':00'
        
    except Exception as e:
        print('Getting now datetime utc. Failed.')
        print(e)
        raise e
    else:
        print('Getting now datetime utc. Success.')
        return result