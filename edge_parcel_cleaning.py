import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from DB import save_to_db

load_dotenv()

def clean_edgecombe_data():
    """
    Cleans the Edgecombe parcels data.
    """
    df = pd.read_csv('edgecombe_parcels.csv')
    df['parcelID'] = df['parcelID'].astype(str)
    df['parcelID'] = df['parcelID'].str.replace('-', '')
    df['updated_at'] = pd.to_datetime('now')

    # print("Cleaned data preview:")
    # print(df.head())
    # print("\nData types:")
    # print(df.dtypes)
    return df

if __name__ == "__main__":
    # Configuration from .env file
    sql_instance = os.getenv("TS_INSTANCE_NAME")
    sql_db = os.getenv("TS_SQL_DB_NAME")
    table = os.getenv("TS_TABLE_NAME")

    # Operations
    cleaned_df = clean_edgecombe_data()
    save_to_db(cleaned_df, sql_instance, sql_db, table)
