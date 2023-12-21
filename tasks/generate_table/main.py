import os
import logging
from tasks.utils.format_csv.main import generate_relation


def create_table_schema_from_csv(**kwargs) -> dict:
    path_of_csv = kwargs["ti"]
    path_of_csv = path_of_csv.xcom_pull(task_ids="upload_to_gcp", key="csvs_path")
    logging.info(f"Receive the following csvs: {path_of_csv}")
    logging.info(f"Applying generate relation")
    return generate_relation(path_of_csv[0])
