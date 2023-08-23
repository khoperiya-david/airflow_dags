import logging
import os
import sys
from datetime import datetime

from airflow.utils.email import send_email

log = logging.getLogger(__name__)

def failure_function(context):
    try:
        # print('context', context)

        if context is None:
            log.error(f"context is null")
            sys.exit(os.EX_SOFTWARE)

        dag_run = context.get('dag_run')
        dag_exception = str(context.get('exception'))[:1000]
        dag_name = context.get('dag').dag_id
        dag_execution_date = context.get('data_interval_start')
        dag_execution_date = dag_execution_date.strftime('%Y-%m-%d %H:%M')

        # print("dag_name", dag_name)
        log.info(f"dag_name {dag_name}")

        
        task_name =""
        msg = ""
        msg_task_table = f"<table><tr><th>Task</th><th>Execution date</th><th>Status</th><tr>"
        msk_task_full = ""
        ti =  context.get('task_instance')  
        for task in ti.get_dagrun().get_task_instances(): #state=TaskInstanceState.FAILED

            # print("task.task_id", task.task_id)
            # print("task.execution_date", task.execution_date)
            # print("task.state", task.state)

            log.info(f"task.task_id {task.task_id}")
            log.info(f"task.execution_date {task.execution_date}")
            log.info(f"task.state {task.state}")

            msg_task_table += f"<tr>"
            msg_task_table += f"<td>{task.task_id} </td>"
            msg_task_table += f"<td>{task.execution_date.strftime('%Y-%m-%d %H:%M')}</td>"
            msg_task_table += f"<td>{task.state}</td></tr>"

            msk_task_full = f"<div>{task}</div>"
            

            task_name += task.task_id + ";"

        msg_task_table += f"</table><br/>"

        msg += f"<table>"
        msg += f"<tr><td>JOB RUN</td><td>{dag_name}</td></tr>"
        msg += f"<tr><td>STATUS</td><td>Failed</td></tr>"
        msg += f"<tr><td>TASKS</td><td>{task_name}</td></tr>"
        msg += f"<tr><td>Execution date</td><td>{dag_execution_date}</td></tr>"
        msg += f"<tr><td>MESSAGES</td><td>{dag_exception}</td></tr>"
        msg += f"</table>"
        msg += "<br/>"
        msg += msg_task_table
        msg += msk_task_full

        subject = f"DAG {dag_run} Failed"
        subject = f"[The dag failed.] Airflow Dag: '{dag_name}' completed on {dag_execution_date}"

        send_email(to='bi_itsm_alerts@mts.ru', subject=subject, html_content=msg)
        # evtkachev@mts.ru
        # bi_itsm_alerts@mts.ru
    except Exception as err:
        log.error(err)
        sys.exit(err)