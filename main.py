import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv

load_dotenv()

def get_data_from_db(sql_instance, sql_db, table_name):
    """
    Retrieves data from a SQL Server database and returns it as a pandas DataFrame.
    """
    conn_str = f'mssql+pyodbc://{sql_instance}/{sql_db}?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server'
    engine = sqlalchemy.create_engine(conn_str)
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    print(f"Successfully loaded data from {table_name}")
    return df

if __name__ == "__main__":
    sql_instance_name = os.getenv("SQL_INSTANCE_NAME")
    sql_db_name = os.getenv("SQL_DB_NAME")

    # Get Nash data
    nash_df = get_data_from_db(sql_instance_name, sql_db_name, 'nash_parcels')
    print("\nNash Data:")
    print(nash_df.head())

    # Get Edgecombe data
    edge_df = get_data_from_db(sql_instance_name, sql_db_name, 'edgecombe_parcels')
    print("\nEdgecombe Data:")
    print(edge_df.head())
