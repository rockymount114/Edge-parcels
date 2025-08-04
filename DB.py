import sqlalchemy
import pandas as pd

def save_to_db(df, sql_instance_name, sql_db_name, table_name):
    """
    Saves a DataFrame to a SQL Server database.
    """
    conn_str = f'mssql+pyodbc://{sql_instance_name}/{sql_db_name}?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server'
    engine = sqlalchemy.create_engine(conn_str)

    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"\nDataFrame saved to SQL table: {table_name} in database: {sql_db_name}")
