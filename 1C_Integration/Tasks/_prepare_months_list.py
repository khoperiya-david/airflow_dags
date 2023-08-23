def prepare_months_list(date_from: str, date_to: str) -> list:
    try:

        print('[Custom] Preparing months list...')

        # Import required util functions
        from Utils._get_first_day_of_month import get_first_day_of_month
        from Utils._get_last_day_of_month import get_last_day_of_month
        from Utils._count_month_in_period import count_month_in_period
        from Utils._add_months import add_months
        
        # Format date_from to first day of month 
        date_from: str = get_first_day_of_month(date_from)

        # Format date_to to last day of month 
        date_to: str = get_last_day_of_month(date_to)

        # Count months between date_from and date_to
        month_count: int = count_month_in_period(date_from, date_to)

        # Define result list
        months: list = []

        for i in range(0, month_count + 1):
            
            month_start_date: str = add_months(date_from, i)

            month_end_date: str = get_last_day_of_month(month_start_date)

            months.append(
                {
                    'date_from': month_start_date,
                    'date_to': month_end_date
                }
            )
        
    except Exception as e:
        print('[Custom] Preparing months list. Failed')
        print(e)
        raise e
    else:
        print('[Custom] Preparing months list. Success.')
        return months
        
#* -------------------------------------------------------------------------- *#
    
