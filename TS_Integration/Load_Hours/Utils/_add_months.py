def add_months(date: str, month_number: int) -> str:
    try:

        print(f'[Custom] Adding {month_number} months...')

        import datetime
        import calendar 

        current_date: datetime.datetime = datetime.datetime.strptime(
            date, 
            '%Y-%m-%d'
        )

        month: int = current_date.month -1 + month_number
        year: int = current_date.year + month_number // 12
        month: int = month % 12 + 1
        day: int = min(current_date.day, calendar.monthrange(year,month)[1])

        result: str = datetime.date(year, month, day).strftime('%Y-%m-%d')

    except Exception as e:
        print(f'[Custom] Adding {month_number} months. Failed.')
        print(e)
        raise e
    else:
        print(f'[Custom] Adding {month_number} months. Success.')
        return result