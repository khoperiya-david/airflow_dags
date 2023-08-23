from airflow.models.param import Param
from airflow.decorators import dag, task
import datetime
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

@dag(
    dag_id="WIP_Act_Drafts_Formation",
    tags=['WIP', 'PMO_Finance', 'Acts', 'Drafts'],
    description='Formation of WIP act drafts',
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
        "year": Param(
            datetime.datetime.today().year, 
            type='integer', 
            minimum=2021, 
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
        ),
        "attribute_list": Param(
            [
                'Projects_Donors[CustomerCode]',
                'Projects_Donors[ContractorCode]',
                'Projects_Donors[Project Code]',
                'Projects_Donors[RMSProjectID]',
                'Projects_Donors[Project Link RMS]',
                'Projects_Donors[Project Start Date]',
                'Projects_Donors[WIPContractName]',
                'Projects_Donors[WIPSubject]',
                'Projects_Donors[WipContractSum]',
                'Projects_Donors[Max KPI Date G6]',
                'Projects_Donors[HyperionNSS_ID]',
                'Projects_Donors[TaskOebsName]',
                'Projects_Donors[CostSegment Code]',
                'Projects_Donors[ResponsibilityCenterMts]',
                'Projects_Donors[WIPCustomerCluster]',
                'Projects_Donors[Project Type for MTS]',
                'Projects_Donors[WIPContractCount]',
                'Projects_Donors[WIPContractId]',
                'Projects_Donors[WIPCurrency]',
                'Projects_Donors[WIPFrameworkId]',
                'Projects_Donors[WipDescription]',
                'Projects_Donors[WIPContractAddName]',
                'Projects_Donors[WIP Start Period Date]',
                'Projects_Donors[Max Plan Date G6]',
                'Projects_Donors[ResponsibilityCenter]',
                'Projects_Donors[Max KPI Date G6 MTSD]',
                'Projects_Donors[Max Plan Date G6 MTSD]',
                'Projects_Donors[PaymentTypeID]',
                'Projects_Donors[PaymentTypeName]',
                'Projects_Donors[ProjectCalculationTypeId]',
                'Projects_Donors[ProjectCalculationTypeName]',
                'Projects[Project Code]','Projects[RMSProjectID]',
                'Projects[Donor_Acceptor]',
                'Projects[ProjectCalculationTypeId]',
                'Projects[ProjectCalculationTypeName]'
            ], 
            type='array', 
            title='Attributes', 
            description='List of act attributes. One per line.'
        ),
        "measure_list": Param(
            [
                '[FactOverheadCostsAndMarginLeadersMTSD до даты]',
                '[FactRub WIP до даты]',
                '[FactMarginCommandOtherExp до даты]',
                '[FactRub renorm до даты]',
                '[FactMarginExtrSubcontr до даты]',
                '[FactOverheadCostsAndMarginwoLeadersMTSD до даты]',
                '[FactOverheadCostsAndMargin_Lab до даты]',
                '[P3880 командировки до даты]',
                '[FactOverheadCostsAndMarginwithLeaders до даты]',
                '[FactMargin до даты]',
                '[FactOverheadCostsAndMargin_MR27 до даты]',
                '[FactRub_CZ_act до даты]',
                '[FactSubcontrCosts до даты]',
                '[SumXLS до даты]',
                '[FactFOT renorm до даты]',
                '[FactIndFOTwoLeadersMTSD до даты]',
                '[TB_Amount до даты]',
                '[FactIndFOTleadersMTSD до даты]',
                '[FactTotalMarginSubcontr до даты]',
                '[Second_Margin_CZ_act до даты]',
                '[FactIncomeMTSD до даты]',
                '[FactRMSProjectCosts до даты]',
                '[FactIndFOT_Lab до даты]',
                '[Остальной субподряд до даты]',
                '[ФормулаAM_C&M до даты]',
                '[FactFOT_MR27 до даты]',
                '[TB_Sumtotal до даты]',
                '[TB_Margin до даты]',
                '[FactFotRub WIP до даты]',
                '[FactOverheadCostswoLeadersMTSD до даты]',
                '[FactMarginwoLeadersMTSD до даты]',
                '[FactIndRubMTSD до даты]',
                '[FactIndFOTclusterleadMTSD до даты]',
                '[FactIndFOTunitheadMTSD до даты]',
                '[FactIndLeadersMTSD до даты]',
                '[FactOverheadCostsClusterleadMTSD до даты]',
                '[FactMarginClusterleadMTSD до даты]',
                '[FactOverheadCostsUnitheadMTSD до даты]',
                '[FactMarginUnitheadMTSD до даты]',
                '[FactOverheadCosts до даты]',
                '[FactOverheadCostsLab до даты]',
                '[FactMarginLab до даты]',
                '[FactIndRubLab до даты]',
                '[FactRub_CZ_Margin_act до даты]',
                '[FactIndRubSubcontr до даты]',
                '[FactMarginwoOHCostswoLeaders до даты]',
                '[Прочие и командировки до даты]',
                '[Субподряд_Командировки_Прочее до даты]',
                '[ФормулаAM_detailed до даты]',
                '[Формула AM C&M or detailed до даты]',
                '[FactPrevActDetailesSum до даты]',
                '[FactPrevActRMSProjectCosts до даты]',
                '[FactPrevActMargin до даты]',
                '[FactPrevActOverheadCosts до даты]',
                '[FactPrevActOtherExpenses до даты]',
                '[FactCurrentActRMSProjectCosts до даты]',
                '[FactCurrentActMargin до даты]',
                '[FactCurrentActOverheadCosts до даты]',
                '[FactCurrentActOtherExpenses до даты]',
                '[Fact Hours WIP до даты]',
                '[FactRubOverHeadCostAmount до даты]',
                '[FactRubMargin до даты]',
                '[FactHours renorm до даты]',
                '[TB_OverHeadCostAmount до даты]',
                '[P3890 прочие до даты]',
                '[FactMarginCommandExp до даты]',
                '[FactMarginOtherExp до даты]'
            ], 
            type='array', 
            title='Measures', 
            description='List of act measures. One per line.'
        ),
        "force_load": Param(
            0, 
            type='integer', 
            title='Force load', 
            description='0 - no force (without deletion), 1 - force (with deletion)'
        ),
        "calculation_date": datetime.datetime.today().strftime('%Y-%m-%d')
    }
)
def generate_wip_act_drafts():
    """
    Get data from Tabular model, transform it and load to DWH as flat table.
    """
    @task(
        task_id='validate_execution_parameters'
    )
    def check_parameters(**kwargs) -> None: 
        from Tasks._validate_param import validate_params
        validate_params(kwargs['params'])
    
    @task(
        task_id='generate_dax_query'
    )
    def create_dax_query(**kwargs) -> str:
        from Tasks._generate_dax_query import generate_dax_query
        dax_query = generate_dax_query(kwargs['params'])
        return dax_query
    
    @task(
        task_id='genereate_sql_query'
    )
    def create_sql_query(dax_query: str, **kwargs) -> str:
        from Tasks._generate_sql_query import generate_sql_query
        sql_query = generate_sql_query(dax_query)
        return sql_query
    
    @task(
       task_id='extract_data_from_cube' 
    )
    def extract_data(sql_query, **kwargs):
        from Tasks._get_data_from_linked_server import get_data
        dataframe_raw = get_data(sql_query, kwargs['params'])
        return dataframe_raw
    
    @task(
        task_id='transform_data'
    )
    def transform_data(dataframe, **kwargs):
        from Tasks._transform_dataframe import transform_dataframe
        dataframe_transformed = transform_dataframe(dataframe, kwargs['params'])
        return dataframe_transformed
    
    @task(
        task_id='load_data_to_stg'
    )
    def load_data(dataframe, **kwargs):
        from Tasks._load_data import load_data_to_stage_table
        load_data_to_stage_table(dataframe, kwargs['params'])
        
    @task(
        task_id='load_data_to_dbo'
    )
    def execute_stored_procedure(**kwargs):
        from Tasks._execute_stored_procedure import execute_stored_procedure
        execute_stored_procedure(kwargs['params'])

    dax_query = create_dax_query()
    sql_query = create_sql_query(dax_query)
    dataframe_raw = extract_data(sql_query)
    dataframe_transformed = transform_data(dataframe_raw)

    check_parameters() >> dax_query >> sql_query >> dataframe_raw >> dataframe_transformed >> load_data(dataframe_transformed) >> execute_stored_procedure()
    
    
generate_wip_act_drafts()
