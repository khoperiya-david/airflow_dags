from airflow.models.param import Param
from airflow.decorators import dag, task
import datetime
import sys
import os


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

@dag(
    dag_id="Post_WIP_Act_Drafts_To_RMS",
    tags=['WIP', 'PMO_Finance', 'Acts', 'Drafts'],
    description='Post WIP act drafts to RMS with API',
    render_template_as_native_obj=True,
    schedule=None,
    start_date=datetime.datetime(2023, 1, 1),
    params={
        "DWH_environment": Param(
            'prod', 
            type='string', 
            title='DWH Environment', 
            description="DWH Environment: prod or test."
        ),
        "RMS_environment": Param(
            'dev', 
            type='string', 
            title='RMS Environment', 
            description="DWH Environment: prod, test or dev."
        ),
        "year": Param(
            datetime.datetime.today().year, 
            type='integer', 
            minimum=2000, 
            maximim=9999, 
            title='Year', 
            description="Year of act drafts"
        ),
        "month": Param(
            datetime.datetime.today().month, 
            type='integer', 
            minimum=1, 
            maximim=12, 
            title='Month', 
            description='Month of act drafts'
        )
    }
)
def post_WIP_act_drafts_to_RMS():
    
    @task(
        task_id='generate_sql_query'
    )
    def generate_sql_query(**kwargs):
        from Tasks._generate_sql_query import generate_sql_query
        sql_query: str = generate_sql_query(kwargs['params'])
        return sql_query
    
    @task(
        task_id='extract_data_from_dwh'
    )
    def extract_data(sql_query: str, **kwargs):
        from Tasks._get_data_from_dwh import get_data
        dataframe = get_data(sql_query, kwargs['params'])
        return dataframe
        
    @task(
        task_id='generate_json'
    )
    def generate_json(dataframe, **kwargs):
        from Tasks._generate_json import generate_json
        json = generate_json(dataframe)
        return json
        
    @task
    def post_data(json, **kwargs):
        from Tasks._post_data_to_rms import post_data_to_rms
        response_code = post_data_to_rms(json, kwargs['params'])
        return response_code
        
    sql_query = generate_sql_query()
    dataframe = extract_data(sql_query)
    json = generate_json(dataframe)
    
    sql_query >> dataframe >> json >> post_data(json)
        
        
post_WIP_act_drafts_to_RMS()
