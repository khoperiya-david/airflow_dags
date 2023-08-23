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

@dag(
    dag_id="TS_Load_Open_Period",
    tags=['TimeSheets 2.0', 'Raw Hours', 'Normalised Hours', 'Hours'],
    description='Load raw and normalised hours open period from TimeSheet 2.0',
    render_template_as_native_obj=True,
    schedule='15 5-20 * * *',
    start_date=datetime.datetime(2023, 1, 1),
    max_active_runs = 1,
    default_args= {
        'on_failure_callback': failure_function
    },
    params={
        "TS_environment": Param(
            'prod', 
            type='string', 
            title='TS Environment', 
            description="TS Environment: prod or test."
        ),
        "DWH_environment": Param(
            'prod', 
            type='string', 
            title='DWH Environment', 
            description="DWH Environment: prod or test."
        ),
        "pool_size": Param(
            6, 
            type='integer', 
            title='API threads', 
            description='Number of API threads'
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
def load_open_period():
    
    @task(
        task_id='define_open_period'
    )
    def get_period(**kwargs):
        """
        Defining the start and end date of the open period 
        and the first day of the year of the open period.
        """
        
        from Tasks._define_period import define_period

        period: dict = define_period(kwargs['params'])

        ti = kwargs["ti"]
        ti.xcom_push("open_period", period)
        
    
    @task(
        task_id='cleare_stg_raw_hours_table'
    )
    def clear_raw(**kwargs):
        """
        Truncate table [stg].[TS_FactTotalHours] with raw hours.
        """
        
        from Tasks._clear_stg_table import clear_stg_table

        clear_stg_table(kwargs['params'], '[stg].[TS_FactTotalHours]')
    
    
    @task(
        task_id='load_raw_hours'
    )
    def load_raw(**kwargs):
        """
        Load raw hours from TS API async.
        """
        
        from Tasks._load_hours import load_hours
        
        import asyncio

        ti = kwargs["ti"]
        period = ti.xcom_pull(task_ids="define_open_period", key="open_period")

        asyncio.run(
            load_hours(period, kwargs['params'], hours_type='raw')
        )
        
        
    @task(
        task_id='cleare_stg_normalised_hours_table'
    )  
    def clear_norm(**kwargs):
        """
        Truncate table [stg].[TS_FactHours] with normalised hours.
        """
        
        from Tasks._clear_stg_table import clear_stg_table

        clear_stg_table(kwargs['params'], '[stg].[TS_FactHours]')
    
    
    @task(
        task_id='load_normalised_hours'
    )   
    def load_norm(**kwargs):
        """
        Load normalised hours from TS API async.
        """
        
        from Tasks._load_hours import load_hours

        import asyncio

        ti = kwargs["ti"]
        period = ti.xcom_pull(task_ids="define_open_period", key="open_period")

        asyncio.run(
            load_hours(period, kwargs['params'], hours_type='normalised')
        )
        
          
    @task(
        task_id='merge_stg_table_data_to_dbo_table'
    )
    def merge_hours(**kwargs):
        """
        Merge raw and normalised hours from stg view to dbo table.
        """

        from Tasks._execute_stored_procedure import execute_stored_procedure

        execute_stored_procedure(
            kwargs['params'], 
            '[dbo].[pr_TS_FactHours_Actualization]'
        )
    
    
    @task(
        task_id='load_task_statuses'
    )
    def update_task_statuses(**kwargs):
        """
        Update task statuses from stg table to dbo table.
        """

        from Tasks._execute_stored_procedure import execute_stored_procedure

        execute_stored_procedure(
            kwargs['params'], 
            '[dbo].[pr_TS_TaskStatus_Actualization]'
        )    
        
        
    @task(
        task_id='update_period_statuses'
    )
    def update_period(**kwargs):
        """
        Update period statuses from TS API to DWH table.
        """

        from Tasks._update_dwh_periods import update_dwh_periods

        update_dwh_periods(kwargs['params'])
        
          
    @task(
        task_id='update_financial_period_statuses'
    )
    def update_fin_period(**kwargs):
        """
        Update financial period statuses from TS API to DWH table.
        """

        from Tasks._execute_stored_procedure import execute_stored_procedure

        execute_stored_procedure(
            kwargs['params'],
            '[dbo].[pr_DWH_Financial_Period_Status_Actualization]'
        )


    @task(
        task_id='define_new_open_period'
    )
    def get_new_period(**kwargs):
        """
        Defining the start and end date of the open period 
        and the first day of the year of the open period.
        """
        
        from Tasks._define_period import define_period

        period: dict = define_period(kwargs['params'])

        ti = kwargs["ti"]
        ti.xcom_push("open_period", period)
    
    
    @task.branch(
        task_id='check_period_changing'
    )
    def check_periods(**kwargs):

        ti = kwargs["ti"]
        period = ti.xcom_pull(task_ids="define_open_period", key="open_period")
        new_period = ti.xcom_pull(
            task_ids="define_new_open_period", 
            key="open_period"
        )

        if period['date_from'] != new_period['date_from']:  
            return 'renormalise_hours'
        else:
            return None
        
         
    @task(
        task_id='renormalise_hours'
    )
    def renormalise_hours(**kwargs):
        """
        Start renormalise when period was closed.
        """

        from Tasks._execute_stored_procedure import execute_stored_procedure

        ti = kwargs["ti"]
        period = ti.xcom_pull(task_ids="define_open_period", key="open_period")

        execute_stored_procedure(
            kwargs['params'],
            (
                f"[dbo].[pr_TS_FactHours_Renormilized_Calculation]"
                f"\'{period['date_from']}\', \'{period['date_to']}\'," 
                f"\'{period['year']}\'"
            )
        )

    @task.branch(
        task_id='check_index_rebuild'
    )
    def check_rebuild_index(**kwargs):
        
        if kwargs['params']['rebuild_index'] == 0:
            return 'update_service_workline'
        else:
            return 'rebuild_index'
        
    @task(
        task_id='rebuild_index'
    )
    def rebuild_index(**kwargs):

        from Tasks._execute_stored_procedure import execute_stored_procedure

        execute_stored_procedure(
            kwargs['params'], 
            '[dbo].[pr_TS_Rebuild_Index]'
        ) 

    @task(
        task_id='update_service_workline',
        trigger_rule='one_success'
    )
    def update_service_workline(**kwargs):
        """
        Update task statuses from stg table to dbo table.
        """

        from Tasks._execute_stored_procedure import execute_stored_procedure

        execute_stored_procedure(
            kwargs['params'], 
            '[dbo].[pr_DWH_FactHours_Service_WorkLines_Actualization]'
        ) 

    @task(
        task_id='rms_service_hours'
    )
    def aggregate_service_workline(**kwargs):
        """
        Update task statuses from stg table to dbo table.
        """

        from Tasks._execute_stored_procedure import execute_stored_procedure

        from Utils._get_first_day_of_year import get_first_day_of_year

        ti = kwargs["ti"]
        period = ti.xcom_pull(task_ids="define_open_period", key="open_period")

        date: str = get_first_day_of_year(period['date_from'])

        execute_stored_procedure(
            kwargs['params'], 
            f"[dbo].[pr_RMS_ServiceHours_Aggregated_YEARS] @startDate = '{date}'"
        )
    

    # Tasks and Task Groups
    get_period = get_period()

    with TaskGroup('raw_hours') as raw_hours:
        clear_raw() >> load_raw()

    with TaskGroup('normalised_hours') as normalised_hours:
        clear_norm() >> load_norm()

    merge_hours = merge_hours()

    update_task_statuses = update_task_statuses()

    update_period = update_period()

    update_fin_period = update_fin_period()

    get_new_period = get_new_period()

    check_periods = check_periods()

    renormalise_hours = renormalise_hours()

    check_rebuild_index = check_rebuild_index()

    rebuild_index = rebuild_index()

    update_service_workline = update_service_workline()

    aggregate_service_workline = aggregate_service_workline()


    # Task Flow
    get_period >> [raw_hours, normalised_hours] >> merge_hours

    merge_hours >> update_task_statuses >> [update_period, check_rebuild_index]

    update_period >> update_fin_period >> get_new_period >> check_periods

    check_periods >> renormalise_hours

    check_rebuild_index >> [rebuild_index, update_service_workline]

    rebuild_index >> update_service_workline >> aggregate_service_workline
 
load_open_period()

