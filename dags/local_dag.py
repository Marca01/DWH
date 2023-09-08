import sys

# import os
# sys.path.insert(0,os.path.abspath(os.path.dirname(__name__)))
sys.path.append("/home/airflow/.local/lib/python3.10/site-packages/datahub_provider")

import os
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
from datahub.api.graphql.operation import Operation
from datahub_provider.operators.datahub_operation_sensor import (
    DataHubOperationCircuitBreakerSensor,
)
from datahub_provider.hooks.datahub import DatahubRestHook
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
from dotenv import load_dotenv

load_dotenv()

GS_CLIENT_SECRET = str(os.getenv("GS_CLIENT_SECRET"))

# from main import uploadSeason, uploadPlayerInfo, uploadPlayerStats, uploadClubInfo, uploadClubStats
# from etl import create_spark_session, main, extract_data, transform_data, load_data

# ┌───────────── minute (0 - 59)
# │ ┌───────────── hour (0 - 23)
# │ │ ┌───────────── day of the month (1 - 31)
# │ │ │ ┌───────────── month (1 - 12)
# │ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday;
# │ │ │ │ │                                   7 is also Sunday on some systems)
# │ │ │ │ │
# │ │ │ │ │
# * * * * * <command to execute>

# A DAG represents a workflow, a collection of tasks
# if want trigger when 2023/05/12 then set start_date = 2023/05/11
# SNOWFLAKE_CONN_ID = 'epl_snowflake_conn'
# #
# default_args = {
#     "snowflake_conn_id": SNOWFLAKE_CONN_ID,
# }

# def custom_failure_email(context):
#     dag_id = context['dag'].dag_id
#     task_id = context['task_instance'].task_id
#     execution_date = context['execution_date']
#     log_url = context['task_instance'].log_url
#     exception = context['exception']
#
#     subject = f"AIRFLOW PIPELINE ALERT!!!: {dag_id}.{task_id} {execution_date} failed"
#     content = f"""
#     Task {task_id} in DAG {dag_id} failed on {execution_date}.
#     Exception: {exception}
#     Log: {log_url}
#     """
#     send_email(to=["khleee00@gmail.com"], subject=subject, html_content=content)

current_season = "2023-24"


def report_operation(context):
    hook: DatahubRestHook = DatahubRestHook("datahub_rest_default")
    host, password, timeout_sec = hook._get_config()
    reporter = Operation(datahub_host=host, datahub_token=password, timeout=timeout_sec)
    task = context["ti"].task
    for outlet in task.outlets:
        print(f"Reporting insert operation for {outlet.urn}")
        reporter.report_operation(urn=outlet.urn, operation_type="UPDATE")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "email": ["khleee00@gmail.com"],
    # "email_on_failure": True,
    # "execution_timeout": timedelta(minutes=5),
    # "on_failure_callback": custom_failure_email,
}

with DAG(
        dag_id="epl_dag",
        start_date=datetime(2023, 8, 28),
        schedule="30 20 * * 1",
        catchup=False,
        default_args=default_args,
) as dag:
    # Ingest data
    t1 = PythonOperator(
        task_id="get_and_upload_season_data", python_callable=upload_season
    )
    t2 = PythonOperator(
        task_id="get_and_upload_player_info_data", python_callable=upload_player_info
    )
    t3 = PythonOperator(
        task_id="get_and_upload_player_stats_data", python_callable=upload_player_stats
    )
    t4 = PythonOperator(
        task_id="get_and_upload_club_info_data", python_callable=upload_club_info
    )
    t5 = PythonOperator(
        task_id="get_and_upload_club_stats_data", python_callable=upload_club_stats
    )
    t6 = PythonOperator(
        task_id="get_and_upload_manager_info_data", python_callable=upload_manager_info
    )
    t7 = PythonOperator(
        task_id="get_and_upload_award_data", python_callable=upload_award
    )
    t8 = PythonOperator(
        task_id="get_and_upload_award_contents_data",
        python_callable=upload_award_contents,
    )
    t9 = PythonOperator(
        task_id="get_and_upload_match_info_data", python_callable=upload_match_info
    )
    t10 = PythonOperator(
        task_id="get_and_upload_match_stats_data", python_callable=upload_match_stats
    )
    t11 = EmptyOperator(task_id="finish_upload_to_data_lake_successfully")

    # # ETL
    t12 = BashOperator(
        task_id="etl",
        # #    bash_command="spark-submit --py-files '/opt/airflow/dags/tasks/utilss.py' --py-files '/opt/airflow/dags/tasks/gx.py' --packages net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.1000 --conf spark.hadoop.fs.s3a.access.key=AKIA5CWFLZTSBUFB6OMB --conf spark.hadoop.fs.s3a.secret.key=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  --conf spark.driver.memory=7g --conf spark.executor.memory=7g /opt/airflow/dags/tasks/etl.py /opt/airflow/dags/tasks/etl.py"
        bash_command=f'spark-submit --py-files "/opt/airflow/dags/tasks/gx.py" --py-files "/opt/airflow/dags/tasks/utilss.py" --packages net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3 --jars https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem --conf spark.hadoop.fs.gs.auth.service.account.enable=true --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/airflow/dags/ServiceKey_GoogleCloudStorage.json --conf spark.hadoop.fs.gs.auth.client.id=1033571263880-bji2ou5204r6a4umbsplhielepae8b4d.apps.googleusercontent.com --conf spark.hadoop.fs.gs.auth.client.secret={GS_CLIENT_SECRET} /opt/airflow/dags/tasks/etl.py'
    )
    t13 = BashOperator(
        task_id="load_gcs",
        dag=dag,
        inlets=[
            Dataset("gcs", f"epl-it/award/{current_season}/month/award.json"),
            Dataset("gcs", f"epl-it/award/{current_season}/season/award.json"),
            Dataset("gcs", f"epl-it/club/{current_season}/club_info.json"),
            Dataset("gcs", f"epl-it/club/{current_season}/club_stats.json"),
            Dataset("gcs", f"epl-it/manager/{current_season}/manager_info.json"),
            Dataset("gcs", f"epl-it/match/{current_season}/match_info.json"),
            Dataset("gcs", f"epl-it/player/{current_season}/player_info.json"),
            Dataset("gcs", f"epl-it/player/{current_season}/player_stats.json"),
            Dataset("gcs", "epl-it/season/seasons.json"),
        ],
        outlets=[
            Dataset("snowflake", "epl.warehouse.dim_award"),
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
            Dataset("snowflake", "epl.warehouse.fct_player_stat"),
        ],
        bash_command="echo Dummy Task",
        on_success_callback=report_operation,
    )
    t14 = EmptyOperator(task_id="finish_etl_successfully")

    # Set dependencies between tasks
    [t1, t2, t3, t4, t5, t6, t7, t8, t9, t10] >> t11 >> t12 >> t13 >> t14
