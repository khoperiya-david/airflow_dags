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
    dag_id="RMS_Load_PlanHours",
    tags=['RMS', 'Plan', 'Hours'],
    description='Load projects plan hours from RMS',
    render_template_as_native_obj=True,
    schedule='30 5-20 * * *',
    start_date=datetime.datetime(2023, 1, 1),
    max_active_runs = 1,
    default_args= {
         'on_failure_callback': failure_function
    },
    params={
        "RMS_environment": Param(
            'prod', 
            type='string', 
            title='RMS Environment', 
            description="RMS Environment: prod or test."
        ),
        "DWH_environment": Param(
            'prod', 
            type='string', 
            title='DWH Environment', 
            description="DWH Environment: prod or test."
        ),
        "full_load": Param(
            0, 
            type='integer',
            minimum=0,
            maximum=1,
            title='Full load',
            description='Full load. 1 - true, 0 - false.'
        )
    }
)
def load_plan_hours():
    
    @task(
        task_id='define_execution_datetime'
    )
    def define_execution_datetime(**kwargs):
        
        from Utils._get_now_datetime_utc import get_now_datetime_utc
        
        execution_datetime: str = get_now_datetime_utc()
        
        print(execution_datetime)
        
        ti = kwargs["ti"]
        ti.xcom_push("execution_datetime", execution_datetime)
    
    
    @task(
        task_id='define_loading_start_date'
    )
    def define_loading_start_date(**kwargs):
        
        from Tasks._define_loading_start_date import define_loading_start_date
        
        last_update_date: str = define_loading_start_date(kwargs['params'])
        
        print(last_update_date)  
        
        ti = kwargs["ti"]
        ti.xcom_push("last_update_date", last_update_date)      
    
    
    @task(
        task_id='clear_stg_table'
    )
    def clear_stg_table(**kwargs):
        
        from Tasks._clear_table import clear_table
        
        clear_table(kwargs['params'], '[stg].[RMS_PlanHours_History]')
    
    
    @task(
        task_id='load_plan_hours'
    )
    def load_plan_hours(**kwargs):
        
        ti = kwargs["ti"]
        last_update_date = ti.xcom_pull(
            task_ids="define_loading_start_date", 
            key="last_update_date"
        )
        execution_datetime = ti.xcom_pull(
            task_ids="define_execution_datetime", 
            key="execution_datetime"
        )
        
        from Tasks._load_plan_hours import load_plan_hours
        
        load_plan_hours(kwargs['params'], last_update_date, execution_datetime)
        
        
    @task(
        task_id='insert_data_to_dbo'
    )
    def insert_data_to_dbo(**kwargs):
        
        from Tasks._insert_data_to_dbo import insert_data_to_dbo
        from Tasks._clear_table import clear_table
        
        if kwargs['params']['full_load'] == 1:  
            clear_table(kwargs['params'], '[dbo].[RMS_PlanHours_History]')
        
        insert_data_to_dbo(kwargs['params'])
        
    
    @task(
        task_id='update_last_loaded_date'
    )
    def update_last_loaded_date(**kwargs):
        
        ti = kwargs["ti"]
        execution_datetime = ti.xcom_pull(
            task_ids="define_execution_datetime", 
            key="execution_datetime"
        )
        
        from Tasks._update_last_loaded_date import update_last_loaded_date
        
        update_last_loaded_date(kwargs['params'], execution_datetime)
        
        
    
    
    # Tasks
    define_execution_datetime = define_execution_datetime()
    define_loading_start_date = define_loading_start_date()
    clear_stg_table = clear_stg_table()
    load_plan_hours = load_plan_hours()
    insert_data_to_dbo = insert_data_to_dbo()
    update_last_loaded_date = update_last_loaded_date()
    
    
    # Task flow
    define_execution_datetime >> define_loading_start_date
    define_loading_start_date >> clear_stg_table
    clear_stg_table >> load_plan_hours >> insert_data_to_dbo
    insert_data_to_dbo >> update_last_loaded_date
 
load_plan_hours() 

