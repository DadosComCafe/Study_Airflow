from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres import PostgresSensor
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from tasks.kaggle.main import download_csv

with DAG(
    dag_id="get_files_from_kaggle",
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 0 * * *",
    catchup=False,
) as dag:

    # getting variables
    kaggle_credentials = {
        "kaggle_user": Variable.get("kaggle_user"),
        "kaggle_password": Variable.get("kaggle_password"),
    }
    mongodb_credentials = {
        "host": Variable.get("mongo_host"),
        "user": Variable.get("mongo_user"),
        "password": Variable.get("mongo_password"),
        "port": Variable.get("mongo_port"),
        "schema": "airflow_tasks"
    }

#     mongo_sensor = MongoSensor(
#     task_id='mogo_sensor_task',
#     mongo_conn_id='mongo_connection',
#     collection='initialize_airflow',
#     mongo_db="local",
#     query={'running': 'false'},
#     poke_interval=10,
#     timeout=600,
#     dag=dag,
# )

    task_initialize_dag = BashOperator(
        task_id="initializing_dag", bash_command="echo Initializing Dag!"
    )



    task_download_csv = PythonOperator(
        task_id="getting_csv",
        python_callable=download_csv,
        op_args=[kaggle_credentials, mongodb_credentials],
    )

    # mongo_sensor >> task_initialize_dag >> task_download_csv
    task_initialize_dag >> task_download_csv
