import pandas as pd
import gzip
import psycopg
import os

# Configuration
DB_CONFIG = {
    "user": "postgres",
    "password": "postgres",
    "host": "db",
    "port": "5432",
    "database": "ny_taxi"
}

# File paths
GREEN_TAXI_DATA_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
TAXI_ZONE_LOOKUP_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
GREEN_TAXI_DATA_PATH = "green_tripdata_2019-10.csv.gz"
TAXI_ZONE_LOOKUP_PATH = "taxi_zone_lookup.csv"


def download_data(url, output_path):
    """
    Download data from a URL and save it to the specified path.
    """
    if not os.path.exists(output_path):
        print(f"Downloading {url}...")
        os.system(f"wget {url} -O {output_path}")
    else:
        print(f"File {output_path} already exists. Skipping download.")


def load_csv_to_dataframe(file_path):
    """
    Load a CSV file into a pandas DataFrame.
    """
    if file_path.endswith(".gz"):
        with gzip.open(file_path, "rb") as file:
            df = pd.read_csv(file)
    else:
        df = pd.read_csv(file_path)
    return df


def create_postgres_connection(db_config):
    """
    Create a connection to the PostgreSQL database using psycopg.
    """
    conn = psycopg.connect(
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["database"]
    )
    return conn


def create_table_from_dataframe(df, table_name, conn):
    """
    Create a table in PostgreSQL from a pandas DataFrame.
    """
    cursor = conn.cursor()

    # Map pandas dtypes to PostgreSQL data types
    dtype_mapping = {
        "int64": "BIGINT",
        "float64": "DOUBLE PRECISION",
        "object": "TEXT",
        "datetime64[ns]": "TIMESTAMP",
        "bool": "BOOLEAN"
    }

    # Generate the CREATE TABLE SQL statement
    columns_with_types = []
    for column, dtype in df.dtypes.items():
        postgres_type = dtype_mapping.get(str(dtype), "TEXT")  # Default to TEXT if dtype is not mapped
        columns_with_types.append(f"{column} {postgres_type}")

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {", ".join(columns_with_types)}
    );
    """

    # Execute the CREATE TABLE query
    cursor.execute(create_table_query)
    conn.commit()
    print(f"Table {table_name} created successfully.")


def insert_data_from_dataframe(df, table_name, conn):
    """
    Insert data from a DataFrame into a PostgreSQL table.
    """
    cursor = conn.cursor()

    # Generate the INSERT query
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    # Insert each row
    for row in df.itertuples(index=False, name=None):
        cursor.execute(insert_query, row)

    conn.commit()
    print(f"Data inserted into {table_name} successfully.")


def main():
    # Step 1: Download data
    download_data(GREEN_TAXI_DATA_URL, GREEN_TAXI_DATA_PATH)
    download_data(TAXI_ZONE_LOOKUP_URL, TAXI_ZONE_LOOKUP_PATH)

    # Step 2: Load data into DataFrames
    green_taxi_df = load_csv_to_dataframe(GREEN_TAXI_DATA_PATH)
    taxi_zone_df = load_csv_to_dataframe(TAXI_ZONE_LOOKUP_PATH)

    # Step 3: Create PostgreSQL connection
    conn = create_postgres_connection(DB_CONFIG)

    # Step 4: Create tables and insert data
    create_table_from_dataframe(green_taxi_df, "green_taxi_data", conn)
    create_table_from_dataframe(taxi_zone_df, "dim_taxi_zone", conn)

    # Step 5: Insert data into tables
    insert_data_from_dataframe(green_taxi_df, "green_taxi_data", conn)
    insert_data_from_dataframe(taxi_zone_df, "dim_taxi_zone", conn)

    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()