import csv
from psycopg2 import connect


def get_values_to_insert_from_csv(csv_path: str):
    csv_reader = csv.DictReader(csv_path)
    # TODO
    # Pensar numa lógica para coletar os dados que serão inseridos no postgres a partir do csv


def populate_table(postgres_credentials: dict, **kwargs):
    csv_path = kwargs["ti"]
    csv_path = csv_path.xcom_pull(task_ids="upload_to_gcp", key="csvs_path")
    try:
        conn = connect(
            database=postgres_credentials["dbname"],
            host=postgres_credentials["host"],
            user=postgres_credentials["user"],
            password=postgres_credentials["password"],
            port=postgres_credentials["port"],
        )
        cur = conn.cursor()
        cur.execute("table_schema")
        conn.commit()
        conn.close()
    except Exception as e:
        raise Exception(f"An error: {e}")
