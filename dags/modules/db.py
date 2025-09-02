from typing import Iterable, Sequence, Any, Optional

from psycopg2.extras import execute_values
from airflow.providers.postgres.hooks.postgres import PostgresHook  

def get_pg_hook(postgres_conn_id: str = "my_postgres") -> PostgresHook:
    """
    Utility function to create and return a PostgresHook.
    
    Args:
        postgres_conn_id (str): The Airflow connection ID for the Postgres database.
    
    Returns:
        PostgresHook: An instance of PostgresHook connected to the specified database.
    """
    return PostgresHook(postgres_conn_id=postgres_conn_id)

def fetch_all(sql: str, conn_id: str = "my_postgres") -> list[tuple[Any, ...]]:
    """
    Fetch all records from a SQL query using PostgresHook.
    
    Args:
        sql (str): The SQL query to execute.
        conn_id (str): The Airflow connection ID for the Postgres database.
    
    Returns:
        list[tuple[Any, ...]]: A list of tuples representing the fetched records.
    """
    hook = get_pg_hook(conn_id)
    return hook.get_records(sql)    

def run_sql(sql: str, parameters: Optional[Sequence[Any]] = None, conn_id: str = "my_postgres") -> None:
    """
    Execute a SQL command using PostgresHook.
    
    Args:
        sql (str): The SQL command to execute.
        parameters (Optional[Sequence[Any]]): Optional parameters to pass to the SQL command.
        conn_id (str): The Airflow connection ID for the Postgres database.
    """
    hook = get_pg_hook(conn_id)
    hook.run(sql, parameters=parameters)

def upsert_values(sql: str, rows: Iterable[Sequence], conn_id: str = "my_postgres", page_size: int = 1000) -> None:
    """
    Execute a single INSERT ... ON CONFLICT ... statement in pages using execute_values.
    `sql` should contain a single %s placeholder for the VALUES block.

    Example sql:
      INSERT INTO public.example_output (id, value)
      VALUES %s
      ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value
    """
    hook = get_pg_hook(conn_id)
    conn = hook.get_conn()
    # Using the connection & cursor as context managers:
    # - on success, it COMMITs
    # - on exception, it ROLLBACKs and raises
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=page_size)
    finally:
        if conn:
            conn.close()    
    
