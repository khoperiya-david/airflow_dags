def transform_period(params: dict) -> dict:
    try:

        print('[Custom] Transform date period...')

        from Utils._get_first_day_of_month import get_first_day_of_month
        from Utils._get_last_day_of_month import get_last_day_of_month

        date_from: str = get_first_day_of_month(params['date_from'])
        
        date_to: str = get_last_day_of_month(params['date_to'])
        
        period: dict = {
            'date_from': date_from,
            'date_to': date_to
        }

    except Exception as e:
        print('[Custom] Transform date period. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Transform date period. Success.')
        return period