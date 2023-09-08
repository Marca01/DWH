import sys

# import os
# sys.path.insert(0,os.path.abspath(os.path.dirname(__name__)))
sys.path.append("/home/airflow/.local/lib/python3.10/site-packages/datahub_provider")

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.lineage.entities import File
from airflow.utils.email import send_email
from airflow.operators.email import EmailOperator
from datahub_provider.entities import Dataset, Urn
from datahub_provider.operators.datahub_operation_sensor import (
    DataHubOperationCircuitBreakerSensor,
)
from tasks.main import (
    upload_season,
    upload_player_info,
    upload_player_stats,
    upload_club_info,
    upload_club_stats,
    upload_manager_info,
    upload_award,
    upload_award_contents,
    upload_match_info,
    upload_match_stats,
)

# ┌───────────── minute (0 - 59)
# │ ┌───────────── hour (0 - 23)
# │ │ ┌───────────── day of the month (1 - 31)
# │ │ │ ┌───────────── month (1 - 12)
# │ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday;
# │ │ │ │ │                                   7 is also Sunday on some systems)
# │ │ │ │ │
# │ │ │ │ │
# * * * * * <command to execute>

# def task_failure_alert(context):
#    dag_id = context['dag'].dag_id
#    task_id = context['task_instance'].task_id
#    execution_date = context['execution_date']
#    log_url = context['task_instance'].log_url
#    exception = context['exception']

#    subject = f"AIRFLOW PIPELINE ALERT!!!: {dag_id}.{task_id} {execution_date} failed"
#    content = f"""
#      Task {task_id} in DAG {dag_id} failed on {execution_date}.
#      Exception: {exception}
#      Log: {log_url}
#      """
#    send_email(to=["kle99680@gmail.com"], subject=subject, html_content=content)

def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    subject = f"AIRFLOW PIPELINE ALERT!!!: {dag}.{task_id} {execution_date} failed"
    content = f"""
      Task {task_list} in DAG {dag} failed on execution_date.
      This task takes longer than 5 minutes to complete.
      """
    send_email(to=["kle99680@gmail.com"], subject=subject, html_content=content)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "email_on_failure": True,
    # "email": ["khleee00@gmail.com"],
    # "execution_timeout": timedelta(minutes=5),
    # "on_failure_callback": custom_failure_email,
}

with DAG(
        dag_id="email_dag",
        start_date=datetime(2023, 8, 28),
        schedule="30 21 * * 1",
        catchup=False,
        default_args=default_args,
        sla_miss_callback=sla_miss_alert
) as dag:
    t1 = DataHubOperationCircuitBreakerSensor(
        task_id="epl_operation_sensor",
        datahub_rest_conn_id="datahub_rest_default",
        urn=[
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,epl.warehouse.dim_award,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,epl.warehouse.dim_club,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,epl.warehouse.dim_country,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,epl.warehouse.dim_manager,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,epl.warehouse.dim_match,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,epl.warehouse.dim_player,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,epl.warehouse.dim_position,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,epl.warehouse.dim_season,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,epl.warehouse.dim_stadium,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,epl.warehouse.fct_club_stat,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,epl.warehouse.fct_object_award,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,epl.warehouse.fct_player_stat,PROD)"
        ],
        time_delta=timedelta(hours=1),
        sla=timedelta(minutes=5)
    )

    t2 = EmailOperator(
        task_id="send_email",
        inlets=[
            Dataset("snowflake", "epl.warehouse.dim_season"),
            Dataset("snowflake", "epl.warehouse.dim_club"),
            Dataset("snowflake", "epl.warehouse.dim_country"),
            Dataset("snowflake", "epl.warehouse.dim_manager"),
            Dataset("snowflake", "epl.warehouse.dim_match"),
            Dataset("snowflake", "epl.warehouse.dim_player"),
            Dataset("snowflake", "epl.warehouse.dim_position"),
            Dataset("snowflake", "epl.warehouse.dim_season"),
            Dataset("snowflake", "epl.warehouse.dim_stadium"),
            Dataset("snowflake", "epl.warehouse.fct_club_stat"),
            Dataset("snowflake", "epl.warehouse.fct_object_award"),
            Dataset("snowflake", "epl.warehouse.fct_player_stat")
        ],
        to="kle99680@gmail.com",
        subject=open("/opt/airflow/dags/templates/email/subject_template.txt").read(),
        html_content=open("/opt/airflow/dags/templates/email/html_content_template.html").read(),
        mime_charset="utf-8"
    )

    t1 >> t2
