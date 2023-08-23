# Import airflow modules
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

# Import global util functions
from bi_operators.email_on_failure import failure_function

# Import required libraries
import datetime
import sys
import os

# Define script directory for imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

# Import required util functions
from Utils._get_last_day_of_month import get_last_day_of_month
from Utils._get_first_day_of_month import get_first_day_of_month


@dag(
    dag_id="PayFact_OPEX_1CUH",
    tags=['1C', 'UH'],
    description='Load PayFact Opex from 1C UH.',
    render_template_as_native_obj=True,
    schedule='10 22 * * *',
    start_date=datetime.datetime(2023, 1, 1),
    max_active_runs = 1,
    default_args= {
        'on_failure_callback': failure_function
    },
    params={
        "1C_UH_environment": Param(
            'prod', 
            type='string', 
            title='1C UH Environment', 
            description="1C UH Environment: prod or test."
        ),
        "DWH_environment": Param(
            'test', 
            type='string', 
            title='DWH Environment', 
            description="DWH Environment: prod or test."
        ),
        "date_from": Param(
            get_first_day_of_month(
                datetime.datetime.today().strftime('%Y-%m-%d')
            ), 
            type='string',
            title='Period date from', 
            description='Period date from.'
        ),
        "date_to": Param(
            get_last_day_of_month(
                datetime.datetime.today().strftime('%Y-%m-%d')
            ), 
            type='string',
            title='Period date to', 
            description='Period date to.'
        ),
        "rebuild_index": Param(
            0, 
            type='integer',
            minimum=0,
            maximum=1,
            title='Index rebuild',
            description='Index rebuild. 1 - need rebuild, 0 - no need rebuild.'
        )
    }
)
def load_pay_fact_opex():
    
    @task(
        task_id='prepare_months_list'
    )
    def prepare_months_list(**kwargs):
        """
        
        """

        from Tasks._prepare_months_list import prepare_months_list

        months: list = prepare_months_list(
            kwargs['params']['date_from'],
            kwargs['params']['date_to']
        )

        print(months)

        ti = kwargs["ti"]
        ti.xcom_push("months", months)


    @task(
        task_id='clear_stg_table'
    )
    def clear_stg_table(**kwargs):
        """
        
        """

        from Tasks._clear_stg_table import clear_stg_table

        clear_stg_table(kwargs['params'], '[stg].[ERP1CUH_PayFact_Opex]')


    @task(
        task_id='load_data_month_by_month_to_stg'
    )
    def load_data_month_by_month_to_stg(**kwargs):
        """
        
        """

        from Tasks._get_data import get_data
        from Tasks._transform_data import transform_data
        from Tasks._load_data import load_data

        import pandas as pd

        ti = kwargs["ti"]
        months: list = ti.xcom_pull(
            task_ids="prepare_months_list", 
            key="months"
        )

        for month in months:
            
            dataframe: pd.DataFrame = get_data(
                month['date_from'], 
                month['date_to'], 
                kwargs['params']
            )

            dataframe = transform_data(dataframe)

            load_data(dataframe, kwargs['params'])


    @task(
        task_id='transfer_data_to_dbo'
    )
    def transfer_data_to_dbo(**kwargs):
        """
        """

        from Tasks._execute_stored_procedure import execute_stored_procedure

        execute_stored_procedure(
            kwargs['params'],
            f"[stg].[pr_ERP1CUH_PayFact_Opex_Actualization] {kwargs['params']['rebuild_index']}"
        )

    @task(
        task_id='actualize_legal_entity'
    )
    def actualize_legal_entity(**kwargs):
        """
        """

        from Tasks._execute_stored_procedure import execute_stored_procedure

        execute_stored_procedure(
            kwargs['params'],
            "[stg].[pr_PMBIWORLD_LegalEntity_Actualization]"
        )

    
    @task(
        task_id='calculate_custom_values'
    )
    def calculate_custom_values(**kwargs):
        """
        """

        from Tasks._execute_stored_procedure import execute_stored_procedure

        execute_stored_procedure(
            kwargs['params'],
            "[dbo].[pr_Calculate_ERP1CUH_PayFact_Opex_CustomValues] 1"
        )



    prepare_months_list = prepare_months_list()
    clear_stg_table = clear_stg_table()
    load_data_month_by_month_to_stg = load_data_month_by_month_to_stg()
    transfer_data_to_dbo = transfer_data_to_dbo()
    actualize_legal_entity = actualize_legal_entity()
    calculate_custom_values = calculate_custom_values()


    prepare_months_list >> clear_stg_table >> load_data_month_by_month_to_stg

    load_data_month_by_month_to_stg >> transfer_data_to_dbo

    transfer_data_to_dbo >> actualize_legal_entity >> calculate_custom_values
 
load_pay_fact_opex()
