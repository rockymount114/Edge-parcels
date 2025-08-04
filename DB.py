import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv
import logging

load_dotenv()
current_dir = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVER_NAME = os.getenv("MU_ADDRESS")
DATABASE_NAME = os.getenv("MU_DATABASE")

def normalize_value(value):
    if isinstance(value, str):
        return value.strip().lower()
    return value

def save_to_db(df, sql_instance_name, sql_db_name, table_name):
    """
    Saves a DataFrame to a SQL Server database.
    """
    conn_str = f'mssql+pyodbc://{sql_instance_name}/{sql_db_name}?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server'
    engine = sqlalchemy.create_engine(conn_str)

    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"\nDataFrame saved to SQL table: {table_name} in database: {sql_db_name}")


def get_munis_parcels():
    server = SERVER_NAME
    database = DATABASE_NAME
    driver = 'ODBC Driver 17 for SQL Server'

    try:
        engine = create_engine(
            f'mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver={driver}',
            connect_args={'autocommit': False}
        )
        with engine.connect() as connection:  # Use context manager for connection
            query = """
                SELECT 
                    d.* 
                FROM (
                    SELECT
                        c.a_ar_customer_cid,
                        RTRIM(LTRIM(b.arbh_parcel))      as arbh_parcel,
                        b.arbh_year,
                        UPPER(RTRIM(LTRIM(c.c_cid_name1))) AS c_cid_name1,
                        UPPER(RTRIM(LTRIM(c.c_cid_name2))) AS c_cid_name2,
                        UPPER(RTRIM(LTRIM(c.c_addr_line1))) AS c_addr_line1,
                        UPPER(RTRIM(LTRIM(c.c_addr_line2))) AS c_addr_line2,
                        UPPER(RTRIM(LTRIM(c.c_cid_city))) AS c_cid_city,
                        UPPER(RTRIM(LTRIM(c.c_cid_state))) AS c_cid_state,
                        CASE 
                            WHEN c_cid_city <> '' THEN CONCAT(UPPER(RTRIM(LTRIM(c.c_cid_city))), ', ', UPPER(RTRIM(LTRIM(c.c_cid_state)))) 
                            ELSE '' 
                        END AS city_st,
                        UPPER(RTRIM(LTRIM(c.c_cid_zip))) AS c_cid_zip,
                        ROW_NUMBER() OVER (PARTITION BY RTRIM(LTRIM(b.arbh_parcel)) ORDER BY b.arbh_year DESC) AS rnk,
                        GETDATE() AS updated_at
                    FROM dbo.ar_customer AS c
                    LEFT JOIN dbo.arbilhdr AS b  
                    ON c.a_ar_customer_cid = b.arbh_acct
                    WHERE b.arbh_parcel <> '' AND LEN(b.arbh_parcel) >= 12
                ) d
                WHERE d.rnk = 1
            """
            df = pd.read_sql(query, connection)

        # Normalize data using apply() and map()
        df = df.apply(lambda col: col.map(normalize_value) if col.dtype == 'object' else col)

        # Strip any remaining whitespace
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()

        # Save to CSV
        df.to_csv(os.path.join(current_dir, 'munis_parcels.csv'), index=False, encoding='utf-8')
        logger.info(f"Data successfully saved to munis_parcels.csv. Total rows: {len(df)}")

    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        raise

    finally:
        engine.dispose()  # Dispose of the engine to release resources
    
    
def get_munis_customer():
    # Get munis customer list, use munis name field to match those empty CITY column, particularly for personal tax in Nash data
    # Not including parcel number as the parcel number has already been used for replace city etc..
    server = SERVER_NAME
    database = DATABASE_NAME
    driver = 'ODBC Driver 17 for SQL Server'

    try:
        engine = create_engine(
            f'mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver={driver}',
            connect_args={'autocommit': False}
        )
        with engine.connect() as connection:        
            query = """SELECT 
                        d.*
                    FROM (
                        SELECT 
                            a_ar_customer_cid
                            , RTRIM(LTRIM(REPLACE(c_cid_name1, ',', ''))) AS c_cid_name1
                            , RTRIM(LTRIM(c_cid_name2)) AS c_cid_name2
                            , RTRIM(LTRIM(c_addr_line1)) AS c_addr_line1
                            , RTRIM(LTRIM(c_addr_line2)) AS c_addr_line2
                            , RTRIM(LTRIM(c_cid_city)) AS c_cid_city
                            , RTRIM(LTRIM(c_cid_state)) AS c_cid_state
                            , CONCAT(RTRIM(LTRIM(c_cid_city)), ', ', RTRIM(LTRIM(c_cid_state))) AS city_st
                            , RTRIM(LTRIM(c_cid_zip)) AS c_cid_zip
                            , c_updated_date
                            , c_updated_by
                            , ROW_NUMBER() OVER (PARTITION BY RTRIM(LTRIM(REPLACE(c_cid_name1, ',', ''))) ORDER BY c_updated_date DESC) AS rnk
                            , GETDATE() AS updated_at
                        FROM dbo.ar_customer
                        WHERE c_cid_name1 IS NOT NULL AND c_cid_name1 <> 'NAM1'
                    ) d
                    WHERE d.rnk = 1
                    """
            df = pd.read_sql(query, engine)
            
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()
            
        df.to_csv(os.path.join(current_dir, 'munis_customer.csv'), index=False, encoding='utf-8')
        logger.info(f"Data successfully saved to munis_customer.csv. Total rows: {len(df)}")
        return df

    except Exception as e:
        print(f"Failed to create database connection: {e}")
        raise

    finally:
        # Only dispose of the engine after all operations
        if 'engine' in locals():
            engine.dispose()