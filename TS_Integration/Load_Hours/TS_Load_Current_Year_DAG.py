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
    dag_id="TS_Load_Current_Year",
    tags=['TimeSheets 2.0', 'Raw Hours', 'Normalised Hours', 'Hours'],
    description=('Load raw and normalised hours current'
                 'and next year from TimeSheet 2.0'),
    render_template_as_native_obj=True,
    schedule='0 3 * * *',
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
            description='Number of API threads.'
        ),
        "date_from": Param(
            str(datetime.datetime.today().year) + '-01-01', 
            type='string',
            title='Period date from', 
            description='Period date from.'
        ),
        "date_to": Param(
            str(datetime.datetime.today().year + 1) + '-12-31', 
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
def load_current_and_next_year():
    
    @task(
        task_id='define_period'
    )
    def get_period(**kwargs):
        """
        Defining the start and end date of the period.
        """
        
        from Tasks._transform_period import transform_period

        period: dict = transform_period(kwargs['params'])

        ti = kwargs["ti"]
        ti.xcom_push("period", period)
    
    
    @task(
        task_id='cleare_stg_raw_hours_table'
    )
    def truncate_raw_stg_table(**kwargs):
        """
        Truncate table [stg].[TS_FactTotalHours] with raw hours.
        """
        
        from Tasks._clear_stg_table import clear_stg_table

        clear_stg_table(kwargs['params'], '[stg].[TS_FactTotalHours]')
    
    
    @task(
        task_id='load_raw_hours'
    )
    def load_raw_hours(**kwargs):
        """
        Load raw hours from TS API async.
        """
        
        from Tasks._load_hours import load_hours

        from Utils._add_months import add_months
        from Utils._count_month_in_period import count_month_in_period
        from Utils._get_last_day_of_month import get_last_day_of_month

        import asyncio

        ti = kwargs["ti"]
        period = ti.xcom_pull(task_ids="define_period", key="period")

        loop_count: int = count_month_in_period(period) 
        
        for i in range(0, loop_count + 1):
            
            date_from: str = add_months(period['date_from'], i)

            date_to: str = get_last_day_of_month(date_from)
            
            current_period = {
                'date_from': date_from,
                'date_to': date_to,
            }

            print(current_period)
            
            asyncio.run(
                load_hours(current_period, kwargs['params'], hours_type='raw')
            )
        
           
    @task(
        task_id='cleare_stg_normalised_hours_table'
    )  
    def truncate_normalised_stg_table(**kwargs):
        """
        Truncate table [stg].[TS_FactHours] with normalised hours.
        """
        
        from Tasks._clear_stg_table import clear_stg_table

        clear_stg_table(kwargs['params'], '[stg].[TS_FactHours]')
    
    
    @task(
        task_id='load_normalised_hours'
    )   
    def load_normalised_hours(**kwargs):
        """
        Load normalised hours from TS API async.
        """
        
        from Tasks._load_hours import load_hours

        from Utils._add_months import add_months
        from Utils._count_month_in_period import count_month_in_period
        from Utils._get_last_day_of_month import get_last_day_of_month

        import asyncio

        ti = kwargs["ti"]
        period = ti.xcom_pull(task_ids="define_period", key="period")

        loop_count: int = count_month_in_period(period) 
        
        for i in range(0, loop_count + 1):
            
            date_from: str = add_months(period['date_from'], i)

            date_to: str = get_last_day_of_month(date_from)
            
            current_period = {
                'date_from': date_from,
                'date_to': date_to,
            }

            print(current_period)
            
            asyncio.run(
                load_hours(
                    current_period, 
                    kwargs['params'], 
                    hours_type='normalised'
                )
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
        period = ti.xcom_pull(task_ids="define_period", key="period")

        date: str = get_first_day_of_year(period['date_from'])

        execute_stored_procedure(
            kwargs['params'], 
            f"[dbo].[pr_RMS_ServiceHours_Aggregated_YEARS] @startDate = '{date}'"
        )
        
 
    # Tasks and Task Groups
    get_period = get_period()
        
    with TaskGroup('raw_hours') as raw_hours:
        truncate_raw_stg_table() >> load_raw_hours()
         
    with TaskGroup('normalised_hours') as normalised_hours:
        truncate_normalised_stg_table() >> load_normalised_hours()
        
    merge_hours = merge_hours()

    update_task_statuses = update_task_statuses()

    check_rebuild_index = check_rebuild_index()

    rebuild_index = rebuild_index()

    update_service_workline = update_service_workline()

    aggregate_service_workline = aggregate_service_workline()


    # Task Flow
    get_period >> [raw_hours, normalised_hours] >> merge_hours

    merge_hours >> update_task_statuses >> check_rebuild_index

    check_rebuild_index >> [rebuild_index, update_service_workline]

    rebuild_index >> update_service_workline >> aggregate_service_workline

        
load_current_and_next_year()
