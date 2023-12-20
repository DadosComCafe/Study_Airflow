from tasks.utils.gcp_wrapper.main import GCPWrapper
import logging
import os

bucket_name = os.getenv("BUCKET_NAME")


def upload_blob_file_to_bucket(**kwargs):
    """Receive files names from the previous task and upload them to cloud.
    Delete the original file from disk once the file has been uploaded successfully.

    Raises:
        Exception: Raises an exception in case of error from the upload_blob_file_to_bucket() GCPWrapper method or
        in delete original file error.
    """
    list_of_files_to_upload = kwargs["ti"].xcom_pull(
        task_ids="getting_dataset", key="dataset"
    )
    logging.info(f"Receive the list of tile to be uploaded: {list_of_files_to_upload}")

    gcp_obj = GCPWrapper(bucket_name=bucket_name)
    for file in list_of_files_to_upload:
        try:
            gcp_obj.upload_blob_file_to_bucket(
                destination_path="teste_csv", filename=file
            )
            logging.info(f"File {file} has been uploaded successfully!")
            os.remove(file)
            logging.info(
                f"The file {file} has been deleted from local disk successfully!"
            )
        except Exception as e:
            raise Exception(f"An exception: {e}")
