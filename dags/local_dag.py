from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

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

with DAG(
        dag_id="epl_dag",
        start_date=datetime(2023, 5, 16),
        schedule="26 23 * * *",
        catchup=False,
        # default_args=default_args
) as dag:

    # Tasks are represented as operators
    # hello = BashOperator(task_id="hello_kh", bash_command="echo hello khale at 22h05")
    # t6 = BashOperator(task_id="hello_2", bash_command="echo testing real dag at 09h10")

    # @task()
    # def airflow():
    #     return 'khale hello airflow at 22h05'\

    # ETL
    t1 = BashOperator(
        task_id="etl",
        bash_command="python3 /opt/airflow/dags/tasks/etl.py"
    )
    t2 = EmptyOperator(
        task_id='end'
    )
    # Set dependencies between tasks
    t1 >> t2
