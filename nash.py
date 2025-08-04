# This script extracts a zip file, converts a DBF file to a Pandas DataFrame, and saves it to a SQL Server database.
# The script assumes that the zip file is in the Downloads folder and the DBF file is in the nash_data folder.
# The script assumes that the SQL Server instance is named DXCH4TSTEST2A and the database is named parcels.
# The script assumes that the DBF file is named PARCELSPRODUCT.DBF.
# The script assumes that the table name is nash_parcels.





import zipfile
import sqlalchemy
import pandas as pd
from simpledbf import Dbf5

def extract_zip(zip_file_path, extract_to_dir):
    """
    Extracts the contents of a zip file to a specified directory.
    """
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_dir)

def process_dbf_to_sql(dbf_path, sql_instance, sql_db_name):
    """
    Reads a DBF file, converts it to a Pandas DataFrame, and saves it to a SQL Server database.
    """
    dbf = Dbf5(dbf_path)
    df = dbf.to_dataframe()
    df['updated_at'] = pd.to_datetime('now')
    
    conn_str = f'mssql+pyodbc://{sql_instance}/{sql_db_name}?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server'
    engine = sqlalchemy.create_engine(conn_str)
    
    
    table_name = 'nash_parcels'
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"DataFrame saved to SQL table: {table_name}")
    print(df.head())

if __name__ == "__main__":
    # Configuration
    zip_file = r"C:\Users\Li\Downloads\Parcels.zip"
    extract_dir = "./nash_data/"
    dbf_file = 'nash_data/PARCELSPRODUCT.DBF'
    sql_instance_name = 'DXCH4TSTEST2A'
    sql_db = 'parcels'

    # Operations
    extract_zip(zip_file, extract_dir)
    process_dbf_to_sql(dbf_file, sql_instance_name, sql_db)







