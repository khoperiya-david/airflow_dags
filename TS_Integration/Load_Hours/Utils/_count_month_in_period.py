def count_month_in_period(period: dict) -> int:
    try:

        print('[Custom] Count month between two dates...')

        import datetime

        

        date_from: datetime.datetime = datetime.datetime.strptime(
            period['date_from'], 
            '%Y-%m-%d'
        )

        date_to: datetime.datetime = datetime.datetime.strptime(
            period['date_to'], 
            '%Y-%m-%d'
        )

        year_delta: int = date_to.year - date_from.year
        month_delta: int = date_to.month - date_from.month
        result: int = year_delta * 12 + month_delta

    except Exception as e:
        print('[Custom] Count month between two dates. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Count month between two dates. Success.')
        return result
