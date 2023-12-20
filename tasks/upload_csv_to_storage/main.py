from tasks.utils.gcp_wrapper.main import GCPWrapper
import os


def upload_blob_file_to_bucket(**kwargs):
    print(kwargs["ti"].xcom_pull(task_ids="getting_dataset", key="dataset"))
    return True
