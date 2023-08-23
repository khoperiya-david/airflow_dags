def get_last_day_of_month(date: str) -> str:
    try:
        
        print('[Custom] Get last day of month...')

        import datetime

        date_to_next_month: datetime.datetime = datetime.datetime.strptime(
            date, 
            '%Y-%m-%d'
        ).replace(day=28) + datetime.timedelta(days=4)
        
        last_day_of_month: datetime.datetime = date_to_next_month - datetime.timedelta(
            days=date_to_next_month.day
        )

        result: str = last_day_of_month.strftime('%Y-%m-%d')

    except Exception as e:
        print('[Custom] Get last day of month. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Get last day of month. Success.')
        return result
