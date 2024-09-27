import os
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def connect_to_postgres():
    """Connect to the PostgreSQL server (to 'postgres' database)"""
    conn = None
    try:
        # Connect to PostgreSQL server using environment variables
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database="postgres",  # Connect to 'postgres' database instead of user's database
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error connecting to PostgreSQL server: {error}")
    return conn


def drop_and_create_database(conn):
    """Drop and recreate the zillow_research database"""
    db_name = os.getenv("DB_NAME", "zillow_research")
    with conn.cursor() as cur:
        # Close existing connections to the database
        cur.execute(f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{db_name}'
            AND pid <> pg_backend_pid();
        """)
        cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
        cur.execute(f"CREATE DATABASE {db_name};")
    print(f"Dropped and recreated database: {db_name}")


def connect_to_db():
    """Connect to the zillow_research database"""
    conn = None
    try:
        # Connect to PostgreSQL server using environment variables
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "zillow_research"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error connecting to zillow_research database: {error}")
    return conn


def create_raw_schema(conn):
    """Create the raw schema if it doesn't exist"""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")


def load_csv_to_table(conn, file_path, table_name):
    """Load a CSV file into a PostgreSQL table"""
    df = pd.read_csv(file_path)

    # Create table with quoted column names
    cols = ", ".join([f'"{col}" TEXT' for col in df.columns])
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS raw.{table_name};")
        cur.execute(f"CREATE TABLE raw.{table_name} ({cols});")

    # Insert data with quoted column names
    columns = [f'"{col}"' for col in df.columns]
    cols = ",".join(columns)
    placeholders = ",".join(["%s"] * len(df.columns))
    query = f"INSERT INTO raw.{table_name}({cols}) VALUES ({placeholders})"

    # Convert dataframe to list of tuples
    tuples = [tuple(x) for x in df.to_numpy()]

    with conn.cursor() as cur:
        from psycopg2.extras import execute_batch

        execute_batch(cur, query, tuples)

    print(f"Loaded {len(df)} rows into raw.{table_name}")


def main():
    # Connect to PostgreSQL server (postgres database)
    server_conn = connect_to_postgres()
    if server_conn is not None:
        # Drop and recreate the database
        drop_and_create_database(server_conn)
        server_conn.close()

    # Connect to the newly created database
    db_conn = connect_to_db()
    if db_conn is not None:
        create_raw_schema(db_conn)

        # Directory containing CSV files
        csv_dir = os.getenv("CSV_DIR", "data/symlinks")

        for filename in os.listdir(csv_dir):
            if filename.endswith(".csv"):
                file_path = os.path.join(csv_dir, filename)
                table_name = os.path.splitext(filename)[0].lower()
                load_csv_to_table(db_conn, file_path, table_name)

        db_conn.close()


if __name__ == "__main__":
    main()
