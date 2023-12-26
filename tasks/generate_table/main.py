import os
import logging
from tasks.utils.format_csv.main import generate_relation
from psycopg2 import connect


def translate_python_to_sql(dict_types: dict) -> dict:
    new_dict = {}
    for key, value in dict_types.items():
        if value == "<class 'float'>":
            new_dict[key] = "DOUBLE PRECISION"
        if value == "<class 'int'>":
            new_dict[key] = "INTEGER"
        if value == "<class 'str'>":
            new_dict[key] = "VARCHAR"
    return new_dict


def create_table_schema_from_csv(**kwargs):
    path_of_csv = kwargs["ti"]
    path_of_csv = path_of_csv.xcom_pull(task_ids="upload_to_gcp", key="csvs_path")
    logging.info(f"Receive the following csvs: {path_of_csv}")
    logging.info(f"Applying generate relation")
    table_schema = [
        generate_relation(path_of_csv[0]),
        path_of_csv[0].replace(".csv", ""),
    ]
    kwargs["ti"].xcom_push(key="table_schema", value=table_schema)


def create_table(table_schema) -> str:
    columns, table_name = table_schema[0], table_schema[1]
    new_columns = {}
    for key, value in columns.items():
        new_columns[
            key.lower()
            .replace("\n", "")
            .replace("(mg)", "")
            .replace("(g)", "")
            .replace(" ", "")
        ] = value

    new_columns = translate_python_to_sql(new_columns)
    columns = ", ".join([f"{column} {new_columns[column]}" for column in new_columns])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
    print(f"Minha query aqui: {create_table_query}")
    return create_table_query


def create_postgres_table_from_schema(postgres_credentials: dict, **kwargs):
    table_schema = kwargs["ti"]
    table_schema = table_schema.xcom_pull(
        task_ids="create_table_schema_from_csv", key="table_schema"
    )
    table_schema = create_table(table_schema)
    logging.info(f"Table schema: {table_schema}")
    try:
        conn = connect(
            database=postgres_credentials["dbname"],
            host=postgres_credentials["host"],
            user=postgres_credentials["user"],
            password=postgres_credentials["password"],
            port=postgres_credentials["port"],
        )
        cur = conn.cursor()
        cur.execute(table_schema)
        conn.commit()
        conn.close()
    except Exception as e:
        raise Exception(f"An error: {e}")
