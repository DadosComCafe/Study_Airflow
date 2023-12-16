from pymongo import MongoClient
import logging


def get_dataset_to_download(**kwargs):
    myclient = MongoClient("mongodb://mongo:senha@mongodb:27017/")
    mydb = myclient["airflow_mongodb"]
    collection = mydb["kaggle_datasets"]

    dataset_to_be_downloaded = collection.find_one({"downloaded": False})
    kwargs["ti"].xcom_push(
        key="dataset", value=dataset_to_be_downloaded["dataset_name"]
    )
    logging.info(f"Get the dataset: {dataset_to_be_downloaded['dataset_name']}")
    return True
