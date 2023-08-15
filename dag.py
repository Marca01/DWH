from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from dags.tasks.main import (
    uploadSeason,
    uploadPlayerInfo,
    uploadPlayerStats,
    uploadClubInfo,
    uploadClubStats,
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
with DAG(
    dag_id="epl_dag",
    start_date=datetime(2023, 5, 13),
    schedule="00 02 * * 1",
    catchup=False,
    # default_args=default_args
) as dag:
    t1 = PythonOperator(
        task_id="get_and_upload_season_data", python_callable=uploadSeason
    )
    t2 = PythonOperator(
        task_id="get_and_upload_player_info_data", python_callable=uploadPlayerInfo
    )
    t3 = PythonOperator(
        task_id="get_and_upload_player_stats_data", python_callable=uploadPlayerStats
    )
    t4 = PythonOperator(
        task_id="get_and_upload_club_info_data", python_callable=uploadClubInfo
    )
    t5 = PythonOperator(
        task_id="get_and_upload_club_stats_data", python_callable=uploadClubStats
    )
    t6 = EmptyOperator(task_id="end")
    # Set dependencies between tasks
    [t1, t2, t3, t4, t5] >> t6
