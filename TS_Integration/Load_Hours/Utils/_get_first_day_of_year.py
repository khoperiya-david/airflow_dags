def get_first_day_of_year(date: str) -> str:
    try:
        
        print('[Custom] Get first day of year...')

        import datetime

        first_day_of_year: str = datetime.datetime.strptime(
            date, 
            '%Y-%m-%d'
        ).replace(day=1).replace(day=1, month=1).strftime('%Y-%m-%d')

    except Exception as e:
        print('[Custom] Get first day of year. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Get first day of year. Success.')
        return first_day_of_year
