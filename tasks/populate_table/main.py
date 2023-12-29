from csv import DictReader
from psycopg2 import connect
import logging


def get_values_to_insert_from_csv(csv_path: str) -> list[tuple]:
    """Get values from csv to be send on postgres table

    Args:
        csv_path (str): The path of csv

    Returns:
        list[tuple]: List of values for each line. Each dictionary values has been transformed as a tuple.
    """
    list_of_queries = []
    with open(csv_path, "r") as file:
        dict_objs = DictReader(file)
        for dicio in dict_objs:
            new_values = tuple(
                "0" if value == "" else value for value in dicio.values()
            )
            list_of_queries.append(new_values)

    print(f"Aqui**lista formada {list_of_queries[0:25]}")
    print(f"Tamanho da lista {len(list_of_queries)}")
    return list_of_queries


def populate_table(postgres_credentials: dict, **kwargs):
    """Populate table using the values returned in a list by the `get_values_to_insert_from_csv` function.

    Args:
        postgres_credentials (dict): The postgres credentials to connect to the database.

    Raises:
        Exception: An exception is raised when there is an error connecting to the database.
    """
    csv_path = kwargs["ti"]
    csv_path = csv_path.xcom_pull(task_ids="upload_to_gcp", key="csvs_path")
    csv_path = csv_path[0]
    table_name = csv_path.replace(".csv", "").lower()
    query_insert = f"INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
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

    list_of_values = get_values_to_insert_from_csv(csv_path)
    error_values = []
    for value in list_of_values:
        try:
            cur.execute(query_insert, value)
            conn.commit()
        except Exception as e:
            error_values.append(value)
            conn.rollback()
            logging.info(f"Value with error: {e}")
        else:
            conn.commit()

    logging.info(f"There are {len(error_values)} invalid values!")
    logging.info(f"{error_values}")
    conn.close()
