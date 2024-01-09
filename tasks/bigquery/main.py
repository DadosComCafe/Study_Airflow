import os
import logging
import pandas as pd
from psycopg2 import connect

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/airflow/credentials/credentials.json"


def realize_get_in_postgres(
    postgres_credentials: dict, list_from_csvs_path: list
) -> tuple:
    table_name = list_from_csvs_path[0].replace(".csv", "").lower()
    query = f"SELECT * FROM {table_name}"
    try:
        conn = connect(
            database=postgres_credentials["dbname"],
            host=postgres_credentials["host"],
            user=postgres_credentials["user"],
            password=postgres_credentials["password"],
            port=postgres_credentials["port"],
        )
        cur = conn.cursor()
    except Exception as e:
        raise Exception(f"An error: {e}")
    cur.execute(query)
    data = cur.fetchall()
    cols = [col[0] for col in cur.description]
    return data, cols


def get_dataframe(fetchall_values: tuple) -> pd.DataFrame:
    records = fetchall_values[0]
    cols = fetchall_values[1]
    df = pd.DataFrame(records, columns=cols)
    return df


def run_bigquery(postgres_credentials: dict, big_query_credentials: dict, **kwargs):
    list_from_csvs_path = kwargs["ti"]
    list_from_csvs_path = list_from_csvs_path.xcom_pull(
        task_ids="upload_to_gcp", key="csvs_path"
    )
    fetchall_values = realize_get_in_postgres(
        postgres_credentials=postgres_credentials,
        list_from_csvs_path=list_from_csvs_path,
    )
    df_values = get_dataframe(fetchall_values)
    try:
        dest_table = big_query_credentials["destination_table"]
        big_project_id = big_query_credentials["project_id"]
        df_values.to_gbq(
            destination_table=dest_table, project_id=big_project_id, if_exists="replace"
        )
        logging.info(f"The query in dataframe: {df_values}")
        logging.info("BigQuery client has been initialized successfully!")
    except Exception as e:
        raise Exception(f"An error: {e}")


if __name__ == "__main__":
    run_bigquery()
