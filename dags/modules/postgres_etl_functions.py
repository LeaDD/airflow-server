from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

def ensure_output_table():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    create_sql = """
        CREATE TABLE IF NOT EXISTS example_output (
            id INT PRIMARY KEY,
            value TEXT
        );
    """

    hook.run(create_sql)

def fetch_data():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    records = hook.get_records("SELECT id, value FROM example_input")

    logging.info(f"Fetched {len(records)} rows from example input.")
    if records:
        logging.info(f"First 3 rows: {records[:3]}")

    return records

def transform(data):
    logging.info("Starting transformation of fetched data.")
    transformed = [(row[0], row[1].upper()) for row in data]
    logging.info(f"Transformed {len(transformed)} rows.")
    if transformed:
        logging.info(f"First 3 transformed rows: {transformed[:3]}")
    return transformed

def write_data(transformed):
    hook = PostgresHook(postgres_conn_id="my_postgres")
    sql = """
        INSERT INTO public.example_output (id, value) 
        VALUES (%s, %s)
        ON CONFLICT (id)
        DO UPDATE SET value = EXCLUDED.value
    """        
    for row in transformed:
        hook.run(sql, parameters=row)