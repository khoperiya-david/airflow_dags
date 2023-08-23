import pandas as pd
import numpy as np

def transform_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    try:

        print('[Custom] Transforming dataframe...')

        numeric_columns: list = dataframe.select_dtypes(
            include=np.number
        ).columns.tolist()

        not_numeric_columns: list = dataframe.select_dtypes(
            exclude=np.number
        ).columns.tolist()

        for numeric_column in numeric_columns:
            if numeric_column not in ('SUM_OPER', 'SUM_RUB', 'RATE_USD_RUB', 'AMOUNT_OLAP'):
                dataframe[numeric_column] = dataframe[numeric_column].fillna('1')
                dataframe[numeric_column] = dataframe[numeric_column].apply(lambda x: str(x))
            else:
                dataframe[numeric_column] = dataframe[numeric_column].fillna(0)

        for not_numeric_column in not_numeric_columns:
            print(not_numeric_column)
            
            if not_numeric_column not in ('ACCOUNTING_DATE', 'PERIOD_OEBS', 'PERIOD', 'DOC_DATE'):
                dataframe[not_numeric_column] = dataframe[not_numeric_column].fillna('1')
            else:
                dataframe[not_numeric_column] = pd.to_datetime(
                    dataframe[not_numeric_column], 
                    yearfirst=True,
                    format='%Y-%m-%dT%H:%M:%S',
                    errors='coerce'
                )
                dataframe[not_numeric_column] = dataframe[not_numeric_column].fillna(pd.Timestamp(year=1970, month=1, day=1))

        # timestamp_columns: list = dataframe.select_dtypes(
        #     include='datetime64'
        # ).columns.tolist()

        # for timestamp_column in timestamp_columns:
        #     dataframe[timestamp_column] = dataframe[timestamp_column].apply(lambda x: x.strftime('%Y-%m-%d %X') + '.0000000')


    except Exception as e:
        print('[Custom] Transforming dataframe. Failed.')
        print(e)
        raise e
    else:   
        print('[Custom] Transforming dataframe. Success.')
        return dataframe 