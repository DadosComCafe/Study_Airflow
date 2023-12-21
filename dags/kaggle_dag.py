from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from tasks.kaggle.main import download_dataset
from tasks.check_from_mongo.main import get_dataset_to_download
from tasks.upload_csv_to_storage.main import upload_blob_file_to_bucket
from tasks.generate_table.main import create_table_schema_from_csv

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
        "schema": "airflow_tasks",
    }

    task_get_dataset_to_download = BranchPythonOperator(
        task_id="get_dataset_to_download",
        python_callable=get_dataset_to_download,
        provide_context=True,
        dag=dag,
    )

    task_initialize_dag = BashOperator(
        task_id="initializing_dag", bash_command="echo Initializing Dag!"
    )

    task_download_dataset = PythonOperator(
        task_id="getting_dataset",
        python_callable=download_dataset,
        provide_context=True,
        op_args=[kaggle_credentials],
        dag=dag,
    )

    task_finalize_dag = BashOperator(
        task_id="finalize_dag", bash_command="echo Finalizing Dag", dag=dag
    )

    task_upload_file_to_storage = PythonOperator(
        task_id="upload_to_gcp",
        python_callable=upload_blob_file_to_bucket,
        provide_context=True,
        dag=dag,
    )

    task_generate_table_schema = PythonOperator(
        task_id="create_table_schema_from_csv",
        python_callable=create_table_schema_from_csv,
        provide_context=True,
        dag=dag,
    )
    task_initialize_dag >> task_get_dataset_to_download
    task_get_dataset_to_download >> [task_download_dataset, task_finalize_dag]
    task_download_dataset >> task_upload_file_to_storage >> task_generate_table_schema
