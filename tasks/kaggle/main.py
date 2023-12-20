import os
import logging


def download_dataset(kaggle_credentials: dict, **kwargs) -> bool:
    """Receive the dataset info from previous task and realize the download of dataset
    from kaggle.

    Args:
        kaggle_credentials (dict): A dict with the kaggle credentials.
        Ex: {"kaggle_user": "myuser", "kaggle_password": "mypassword"}

    Raises:
        Exception: The first exception is raised in authentication error.
        Exception: The second exception is raised in data set download error.

    Returns:
        bool: True if no exception was raised.
    """
    os.environ["KAGGLE_USERNAME"] = kaggle_credentials["kaggle_user"]
    os.environ["KAGGLE_KEY"] = kaggle_credentials["kaggle_password"]
    logging.info(f"The kaggle credentials: {kaggle_credentials}")

    dataset = kwargs["ti"]
    dataset = dataset.xcom_pull(task_ids="get_dataset_to_download", key="dataset")
    logging.info(f"The dataset: {dataset}")
    from kaggle.api.kaggle_api_extended import KaggleApi

    try:
        api = KaggleApi()
        api.authenticate()
        logging.info("Authenticated!")
    except Exception as e:
        raise Exception(f"An error: {e}")

    try:
        list_of_datasets = api.dataset_list(
            search=f"{dataset['dataset_owner']}/{dataset['dataset_name']}"
        )
        logging.info(f"The dataset in list: {list_of_datasets}")
        for dataset in list_of_datasets:
            print(dataset)
            api.dataset_download_files(
                dataset=dataset.ref, path=".", force=False, unzip=True
            )

        list_of_csv = [file for file in os.listdir() if ".csv" in file]
        kwargs["ti"].xcom_push(key="dataset", value=list_of_csv)
        # use xcom_push to pass list_of_csv to next task
        logging.info(f"Here is the list of downloaded csv: {list_of_csv}")
        logging.info("The dataset has been downloaded successfully!")
        logging.info(f"Files in current location: {os.listdir()}")
    except Exception as e:
        raise Exception(f"An error: {e}")
    return True
