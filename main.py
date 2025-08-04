import pandas as pd
import sqlalchemy
import os
import logging
import re
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

def detect_address_format(parts):
    """
    Detect the format of address parts.
    
    Returns:
        str: 'us_standard', 'us_military', 'international', or 'unknown'
    """
    if len(parts) < 2:
        return 'unknown'
    
    # Check if first part looks like a zipcode (5 digits, or 5+4 format)
    first_part = parts[0]
    
    # US Military/Diplomatic format: ZIPCODE CITY STATE (like "09774 DPO AE")
    if (re.match(r'^\d{5}(-\d{4})?$', first_part) and 
        len(parts) == 3 and 
        len(parts[2]) == 2 and 
        parts[1].upper() in ['APO', 'FPO', 'DPO']):
        return 'us_military'
    
    # International format: starts with postal code (digits), ends with country name
    if (re.match(r'^\d+$', first_part) and 
        len(parts) >= 3 and 
        not re.match(r'^\d+$', parts[-1]) and  # Last part is not just digits
        len(parts[-1]) > 2):  # Country names are usually longer than 2 chars
        return 'international'
    
    # US Standard format: CITY STATE ZIPCODE (last part is zipcode)
    last_part = parts[-1].rstrip('-')  # Remove trailing hyphen for check
    if (len(parts) >= 2 and 
        re.match(r'^\d{5}(-\d{4})?$', last_part)):
        return 'us_standard'
    
    return 'unknown'

def splity_city(address_text):
    """
    Split address string into CITY, STATE, ZIPCODE components.
    Handles multiple formats: US standard, US military, and international.
    
    Args:
        address_text (str): Address string
    
    Returns:
        dict: Dictionary with keys 'CITY', 'STATE', 'ZIPCODE', 'COUNTRY', 'FORMAT'
    """
    if pd.isna(address_text) or not address_text.strip():
        return {'CITY': '', 'STATE': '', 'ZIPCODE': '', 'COUNTRY': '', 'FORMAT': 'empty'}
    
    # Split by spaces and extract components
    parts = address_text.strip().split()
    
    # Detect format
    format_type = detect_address_format(parts)
    
    city = ""
    state = ""
    zipcode = ""
    country = ""
    
    if format_type == 'us_standard':
        # US Standard: CITY STATE ZIPCODE
        if len(parts) >= 3:
            zipcode = parts.pop()
            # Remove trailing hyphen only if it's at the end (not part of extended zip format)
            if zipcode.endswith('-') and not re.match(r'\d+-\d+', zipcode):
                zipcode = zipcode[:-1]
            state = parts.pop()
            city = " ".join(parts)
        elif len(parts) == 2:
            zipcode = parts[1]
            if zipcode.endswith('-') and not re.match(r'\d+-\d+', zipcode):
                zipcode = zipcode[:-1]
            state = parts[0]
        elif len(parts) == 1:
            zipcode = parts[0]
            if zipcode.endswith('-') and not re.match(r'\d+-\d+', zipcode):
                zipcode = zipcode[:-1]
        country = "USA"
    
    elif format_type == 'us_military':
        # US Military: ZIPCODE CITY STATE (like "09774 DPO AE")
        zipcode = parts[0]
        city = parts[1]
        state = parts[2]
        country = "USA"
    
    elif format_type == 'international':
        # International: POSTAL_CODE CITY COUNTRY
        zipcode = parts[0]  # Use zipcode field for postal code
        country = parts[-1]
        if len(parts) > 2:
            city = " ".join(parts[1:-1])  # Everything between postal code and country
        else:
            city = ""
        state = ""  # International addresses don't have US states
    
    else:
        # Unknown format - try to make best guess
        if len(parts) >= 2:
            # If last part looks like a country (not digits)
            if not re.match(r'^\d+', parts[-1]):
                country = parts[-1]
                if len(parts) > 2:
                    city = " ".join(parts[:-1])
            else:
                # Assume it's some variation of US format
                zipcode = parts[-1]
                if zipcode.endswith('-') and not re.match(r'\d+-\d+', zipcode):
                    zipcode = zipcode[:-1]
                if len(parts) > 1:
                    city = " ".join(parts[:-1])
                country = "USA"
    
    return {
        'CITY': city, 
        'STATE': state, 
        'ZIPCODE': zipcode, 
        'COUNTRY': country,
        'FORMAT': format_type
    }
    

if __name__ == "__main__":
    
    # Get TS data
    sql_instance_name = os.getenv("TS_INSTANCE_NAME")
    sql_db_name = os.getenv("TS_SQL_DB_NAME")

    # Get Nash data
    nash_df = get_data_from_db(sql_instance_name, sql_db_name, 'nash_parcels')
    
    # Filter out rows where ML_C_ST_Z contains 'RETURNED'
    nash_df_filtered = nash_df[
        ~nash_df['ML_C_ST_Z'].str.contains('RETURNED', na=False) |
        ~nash_df['ML_C_ST_Z'].str.contains('UNKNO', na=False)
    ].copy()
    
    # Apply the splity_city function to the filtered DataFrame
    address_parts = nash_df_filtered['ML_C_ST_Z'].apply(splity_city)
    city_df = pd.DataFrame(address_parts.tolist(), index=nash_df_filtered.index)
    
    # Join the new address columns back to the original DataFrame
    nash_df = nash_df.join(city_df)

    nash_df.to_csv('nash_parcels_with_city.csv', index=False)
    print(nash_df.head())
    
    # # Get Edgecombe data
    # edge_df = get_data_from_db(sql_instance_name, sql_db_name, 'edgecombe_parcels')
    # print("\nEdgecombe Data:")
    # print(edge_df.head())
    
    # # Get Munis data
    # from DB import get_munis_parcels, get_munis_customer
    # get_munis_parcels()
    # get_munis_customer()