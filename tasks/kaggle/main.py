import os
import logging


def download_dataset(kaggle_credentials, mongo_credentials, **kwargs):
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
            api.dataset_download_files(
                dataset=dataset.ref, path=".", force=False, unzip=True
            )
            logging.info("The dataset has been downloaded successfully!")
            logging.info(f"Files in current location: {os.listdir()}")
    except Exception as e:
        raise Exception(f"An error: {e}")
    return True
