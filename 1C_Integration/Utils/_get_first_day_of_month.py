def get_first_day_of_month(date: str) -> str:
    try:
        
        print('[Custom] Get first day of month...')

        import datetime

        first_day_of_month: str = datetime.datetime.strptime(
            date, 
            '%Y-%m-%d'
        ).replace(day=1).strftime('%Y-%m-%d')

    except Exception as e:
        print('[Custom] Get first day of month. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Get first day of month. Success.')
        return first_day_of_month
