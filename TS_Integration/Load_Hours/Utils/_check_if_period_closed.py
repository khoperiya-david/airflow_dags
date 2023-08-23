def check_if_period_closed(period: dict, current_period: dict) -> dict:
    try:
        print('[Custom] Checking if period is closed...')
        if period['date_from'] != current_period['date_from']:  
            flag: bool = True
        else:
            flag: bool = False
    except Exception as e:
        print('[Custom] Checking if period is closed. Failed.')
        print(e)
        raise e
    else:
        print(f'[Custom] Checking if period is closed - {flag}. Succeed.')
        return flag
     
     