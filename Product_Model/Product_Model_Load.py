# Import airflow modules
from airflow.models.param import Param
from airflow.decorators import dag, task

# Import global util functions
from bi_operators.email_on_failure import failure_function

# Import required libraries
import datetime
import sys
import os

# Define script directory for imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

@dag(
    dag_id="Product_Model_View_Formation",
    tags=['Product', 'Model'],
    description='Formation of Product Model view',
    render_template_as_native_obj=True,
    schedule='30 6-18 * * *',
    start_date=datetime.datetime(2023, 1, 1),
    max_active_runs = 1,
    default_args= {
        'on_failure_callback': failure_function
    },
    params={
        "DWH_environment": Param(
            'prod', 
            type='string', 
            title='DWH Environment', 
            description="DWH Environment: prod or test."
        ),
    }
)
def create_product_model_view():

    """
    Airflow DAG, which implements an ETL process to generate a flat table 
    for the Product Model Superset report.
    """ 

    @task(
       task_id='load_data_from_cube_to_dwh',
       execution_timeout=datetime.timedelta(seconds=360) 
    )
    def load_data_from_cube_to_dwh(**kwargs):
        """
        Get data from cube by linked server and write it to flat table in MSSQL.
        """
        
        from Tasks._execute_stored_procedure import execute_stored_procedure

        execute_stored_procedure(
            kwargs['params'], 
            '[dbo].[pr_ProductModel_Actualization]'
        )
    
    
    @task(
        task_id='extract_data_from_dwh'
    )
    def exctract_data_from_dwh(**kwargs):
        """
        Get data from frat table in MSSQL and write it to dataframe.

        Return
        ------
        dataframe_raw: pandas.DataFrame
            Pandas dataframe with data from table
        """
        
        from Tasks._get_data_from_dwh import get_data_from_dwh
        
        dataframe_raw = get_data_from_dwh(kwargs['params'])
        
        return dataframe_raw
    

    @task(
        task_id='load_data_to_clickhouse'
    )
    def load_data_to_clickhouse(dataframe, **kwargs):
        """
        Load data from given dataframe to ClickHouse flat table.

        Parameters
        ------
        dataframe: pandas.DataFrame
            Pandas dataframe with data in it
        """

        from Tasks._load_data import load_data

        load_data(kwargs['params'], dataframe)


    # Tasks
    load_data_from_cube_to_dwh = load_data_from_cube_to_dwh()
    dataframe = exctract_data_from_dwh()
    load_data_to_clickhouse = load_data_to_clickhouse(dataframe)


    # Task flow
    load_data_from_cube_to_dwh >> dataframe
    

create_product_model_view()
