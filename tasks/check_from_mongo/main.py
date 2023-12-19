from pymongo import MongoClient
import logging


def get_dataset_to_download(**kwargs):
    """Checks if there are any kaggle dataset records that have not yet been downloaded"""
    try:
        myclient = MongoClient("mongodb://mongo:senha@mongodb:27017/")
        mydb = myclient["airflow_mongodb"]
        collection = mydb["kaggle_datasets"]

        dataset_to_be_downloaded = collection.find_one({"downloaded": False})

        if not dataset_to_be_downloaded:
            return "finalize_dag"

        kwargs["ti"].xcom_push(
            key="dataset",
            value={
                "dataset_name": dataset_to_be_downloaded["dataset_name"],
                "dataset_owner": dataset_to_be_downloaded["dataset_owner"],
            },
        )
        logging.info(f"Get the dataset: {dataset_to_be_downloaded['dataset_name']}")
        return "getting_csv"
    except Exception as e:
        logging.error(f"An exception: {e}")
