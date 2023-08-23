import pandas as pd
import pyodbc

def load_data(dataframe: pd.DataFrame, params: dict) -> None:
    try:

        print('[Custom] Loading data...')

        from Utils._get_mssql_dsn import get_mssql_dsn
        from Utils._connect_to_dwh import connect_to_dwh

        # Get MSSQL dsn
        mssql_dsn: str = get_mssql_dsn(params)

        # Get MSSQL Connection
        mssql_connection: pyodbc.Connection = connect_to_dwh(mssql_dsn)

        #Transform dataframe to list
        data:list = dataframe.values.tolist()
        
        print(data[0])

        # data: list = [
        #     '1882700', 
        #     'МТС ДИДЖИТАЛ ООО', 
        #     '4001010504', 
        #     'НЕ ИСПОЛЬЗОВАТЬ с 01.07.2023г.!!! Услуги по подрядным договорам для группы МТС – Расходы на субподряд', 
        #     '4001010504', 'НЕ ИСПОЛЬЗОВАТЬ с 01.07.2023г.!!! Услуги по подрядным договорам для группы МТС – Расходы на субподряд', 
        #     '6001010000', 
        #     'Расчеты с поставщиками', 
        #     'RRM', 
        #     'Трайб Ops Platform', 
        #     '206060005.0', 
        #     'Расходы на субподряд - оказание услуг для Группы МТС', 
        #     '1882700223102708', 
        #     '1882700223102708', 
        #     'Внедрение программного продукта «Партнерская среда», а именно адаптация программы для ЭВМ «Наш МТС» с целью дальнейшего взаимодействия с программой дл', 
        #     pd.Timestamp('2023-07-12 07:00:00'), 
        #     '1', 
        #     889200.0, 
        #     889200.0, 
        #     1, 
        #     pd.Timestamp('2023-07-12 07:00:00'), 
        #     '150343', 
        #     'Яковлева Ирина Евгеньевна', 
        #     'МТС ЛАБ ООО', 
        #     'Y', 
        #     pd.Timestamp('2023-07-12 07:00:00'), 
        #     'Услуги', 
        #     'ZПОR04447001', 
        #     'Заявка № 1966 LAB/R04447 от 16.03.2023 к Расходный договор № D200307022 от 16.10.2020 (ООО "МТС ЛАБ"', 
        #     'Договор № D200307022 от 06.11.2020', 
        #     pd.Timestamp('2023-07-12 07:00:00'), 
        #     0.0, 
        #     'Услуги', 
        #     '1', 
        #     '1', 
        #     '1'
        # ]

        # Execute Insert
        _insert_data(data, mssql_connection)

    except Exception as e:
        print('[Custom] Loading data. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Loading data. Success.')

#* -------------------------------------------------------------------------- *#


def _insert_data(data: list, connection: pyodbc.Connection) -> None:
    try:
        
        print('[Custom] Inserting data into table...')
        
        mssql_cursor: pyodbc.Cursor = connection.cursor()

        mssql_cursor.fast_executemany = True

        mssql_cursor.executemany("""
            INSERT INTO [stg].[ERP1CUH_PayFact_Opex] (
                [ETL_LOG_ID],
                [SEGMENT1],
                [SEGMENT1_DESCRIPTION],
                [SEGMENT2],
                [SEGMENT2_DESCRIPTION],
                [SEGMENT2DT],
                [SEGMENT2DT_DESCRIPTION],
                [SEGMENT2KT],
                [SEGMENT2KT_DESCRIPTION],
                [SEGMENT3],
                [SEGMENT3_DESCRIPTION],
                [SEGMENT4],
                [SEGMENT4_DESCRIPTION],
                [SEGMENT5],
                [SEGMENT5_DESCRIPTION],
                [DESCRIPTION],
                [ACCOUNTING_DATE],
                [CURRENCY_CODE],
                [SUM_OPER],
                [SUM_RUB],
                [RATE_USD_RUB],
                [PERIOD],
                [POSTED_NUMBER],
                [AUTHOR_NAME],
                [REFERENCE2],
                [DEBET],
                [PERIOD_OEBS],
                [CATEGORY_NAME],
                [TASK_NUMBER],
                [ORDER_NUMBER],
                [GEN_CONTRACT],
                [DOC_DATE],
                [AMOUNT_OLAP],
                [BI_DATA_TYPE],
                [BI_PROJECTS_DIRECTION],
                [BI_PROJECTS_TYPE],
                [BI_PL_ITEM]
            )
            VALUES(
                0x0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, data)
        
        connection.commit()
        
    except Exception as e:
        print('[Custom] Inserting data into table. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Inserting data into table. Success.')


