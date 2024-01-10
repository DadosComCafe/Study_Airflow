from tasks.utils.mongo_wrapper import Mongo
import logging


def change_downloaded_in_record(mongodb_credentials: dict, **kwargs):
    """Change key value to True in downloaded from given record"""
    data_record = kwargs["ti"]
    data_record = data_record.xcom_pull(
        task_ids="get_dataset_to_download", key="dataset"
    )
    dataset_name = data_record["dataset_name"]
    dataset_owner = data_record["dataset_owner"]
    query = {"dataset_name": dataset_name, "dataset_owner": dataset_owner}
    new_value = {"$set": {"downloaded": True}}
    try:
        mongo_obj = Mongo(credentials=mongodb_credentials)
        logging.info(query)
        logging.info(new_value)
        myclient = mongo_obj.get_client()
        logging.info("OK aqui")
        mydb = myclient[mongodb_credentials["schema"]]
        logging.info("OK aqui")
        collection = mydb["kaggle_datasets"]
        logging.info("OK aqui")
        collection.update_one(filter=query, update=new_value, upsert=False)
        logging.info(f"The record has been changed.")
    except Exception as e:
        logging.error(f"An exception: {e}")
