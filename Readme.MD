# Property Data Management System

A comprehensive Python application for collecting, cleaning, and managing property parcel data from multiple sources including Edge County website, Nash County DBF files, and Munis database.

## Features

- **Web Scraping**: Automatically scrapes parcel data from Edgecombe County tax website
- **DBF Processing**: Extracts and processes Nash County parcel data from DBF files
- **Database Integration**: Connects to Munis database for additional parcel and customer information
- **Data Cleaning**: Advanced address parsing and data standardization
- **Multi-format Support**: Handles US standard, US military, and international address formats

## Prerequisites

- Python 3.8+
- SQL Server with ODBC Driver 17
- Access to Munis database
- Required Python packages (see Installation)

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd propertyMaster
   ```

2. **Install dependencies using uv (recommended)**
   ```bash
   uv sync
   ```
   
   Or using pip:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   
   Create a `.env` file in the project root with the following variables:
   ```env
   # TS Database Configuration
   TS_INSTANCE_NAME=your_ts_instance_name
   TS_SQL_DB_NAME=your_database_name
   TS_TABLE_NAME=edgecombe_parcels
   
   # Munis Database Configuration
   MU_ADDRESS=your_munis_server_address
   MU_DATABASE=your_munis_database_name
   ```

## Usage

### 1. Scrape Edgecombe County Parcel Data

Automatically scrapes all parcel data from the Edgecombe County tax website:

```bash
python main1.py
```

**What it does:**
- Scrapes parcel data from https://taxpa.edgecombecountync.gov/paas/
- Extracts: Parcel ID, Property Address, Owner information, Tax values, etc.
- Saves data to `edgecombe_parcels.csv`
- Includes respectful delays between requests

### 2. Process Nash County DBF Files

Extracts Nash County parcel data from DBF files:

```bash
python nash.py
```

**Requirements:**
- Place the Nash County Parcels.zip file in your Downloads folder
- The script will extract and process `PARCELSPRODUCT.DBF`

**What it does:**
- Extracts zip file to `./nash_data/` directory
- Converts DBF file to pandas DataFrame
- Saves data to SQL Server database as `nash_parcels` table

### 3. Clean and Process Nash Data

Performs advanced data cleaning on Nash parcel data:

```bash
python main.py
```

**Data Cleaning Features:**
- **Address Parsing**: Splits `ML_C_ST_Z` field into separate CITY, STATE, ZIPCODE columns
- **Format Detection**: Automatically detects and handles:
  - US Standard format: "CITY STATE ZIPCODE"
  - US Military format: "ZIPCODE APO/FPO/DPO STATE"
  - International format: "POSTAL_CODE CITY COUNTRY"
- **Data Filtering**: Removes invalid parcel IDs and returned mail addresses
- **Owner Consolidation**: If OWNER1 equals OWNER2, empties OWNER2 field

### 4. Clean Edgecombe Data

Cleans and standardizes Edgecombe parcel data:

```bash
python edge_parcel_cleaning.py
```

**What it does:**
- Removes hyphens from parcel IDs
- Adds timestamp for data tracking
- Saves cleaned data to SQL Server

### 5. Access Munis Database

Retrieve parcel and customer data from Munis database:

```python
from DB import get_munis_parcels, get_munis_customer

# Get parcel data
get_munis_parcels()  # Saves to munis_parcels.csv

# Get customer data  
get_munis_customer()  # Saves to munis_customer.csv
```

## Data Processing Rules

### Nash Parcels Processing
- **Tax Districts**: Filters for TXSDIST IN ('RM00', 'RM62')
- **Owner Deduplication**: If OWNER1 = OWNER2, OWNER2 is emptied
- **Address Standardization**: ML_C_ST_Z field is split into CITY, STATE, ZIPCODE columns
- **Mail Return Handling**: Records with 'MAIL RETURNED' are flagged for Munis data replacement

### Address Format Support
The system intelligently detects and processes multiple address formats:

1. **US Standard**: "RALEIGH NC 27601"
2. **US Military**: "09774 DPO AE" 
3. **International**: "12345 LONDON UK"

## Output Files

- `edgecombe_parcels.csv` - Raw scraped Edgecombe data
- `nash_parcels_with_city.csv` - Processed Nash data with parsed addresses
- `munis_parcels.csv` - Munis parcel data
- `munis_customer.csv` - Munis customer data

## Database Tables

- `edgecombe_parcels` - Cleaned Edgecombe parcel data
- `nash_parcels` - Nash parcel data from DBF files

## Error Handling

- **Network Issues**: Automatic retry logic for web scraping
- **Database Connections**: Proper connection management and cleanup
- **Data Validation**: Filters out malformed or incomplete records
- **Logging**: Comprehensive logging for debugging and monitoring

## Troubleshooting

1. **Database Connection Issues**
   - Verify SQL Server instance name and database name in `.env`
   - Ensure ODBC Driver 17 for SQL Server is installed
   - Check Windows authentication permissions

2. **Web Scraping Issues**
   - Check internet connectivity
   - Verify the Edgecombe County website is accessible
   - Review rate limiting delays if requests fail

3. **DBF File Processing**
   - Ensure the zip file is in the correct Downloads folder path
   - Verify DBF file structure matches expected format


