# Import required libraries
import pandas as pd

def load_data(params:dict, dataframe: pd.DataFrame) -> None:
    try:

        print('[Custom] Loading data to ClickHouse...')

        # Import required Airflow modules
        from airflow.hooks.base_hook import BaseHook

        # Import required libraries
        from clickhouse_driver import Client
        
        # Get ClickHouse connection secrets
        secrets = BaseHook.get_connection('v_clickhouse_bi_dwh_readwrite')

        # Define ClickHouse connection
        client = Client(
            host=secrets.host, 
            database=secrets.schema, 
            user=secrets.login, 
            password=secrets.password
        )

        # Transform pandas dataframe with data in it to list of rows
        data: list = list(dataframe.itertuples(index=False, name=None))

        # Count rows in list
        data_len: int = len(data)

        # Define batch size
        batch_size: int = 50000

        # Calculate the number of steps in the loop
        loop_count: int = data_len // batch_size

        # Clear destination table
        client.execute(
                'TRUNCATE TABLE product_model',
            )

        # Insert data into table by loop
        for i in range(loop_count):
            client.execute(
                'INSERT INTO product_model (* EXCEPT(loaded_at)) VALUES',
                data[i * batch_size: i * batch_size + batch_size]
            )

        # If rows count isn`t a multiple of batch size then insert it separately
        if len(data[loop_count * batch_size: data_len]) > 0:
            client.execute(
                    'INSERT INTO product_model (* EXCEPT(loaded_at)) VALUES',
                    data[loop_count * batch_size: data_len]
                )

    except Exception as e:
        print('[Custom] Loading data to ClickHouse. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Loading data to ClickHouse. Success.')