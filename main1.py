import time
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup

# This script scrapes all parcel data from the Edgecombe County tax website
# and displays it in a Pandas DataFrame.

def scrape_edgecombe_parcels():
    """
    Scrapes all parcel data and returns it as a Pandas DataFrame.
    """
    url = "https://taxpa.edgecombecountync.gov/paas/"
    headers = {
        'authority': 'taxpa.edgecombecountync.gov',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
    }
    # Base payload for the search
    base_payload = [
        ('OWNER', ''),
        ('MAP', ''),
        ('lACRE', ''),
        ('uACRE', ''),
        ('SUB', ''),
        ('TWP', ''),
        ('TAX', 'G01'),
        ('col', 'Parcel ID'),
        ('col', 'Property Address'),
        ('col', 'Property Description'),
        ('col', 'Current Owner 1'),
        ('col', 'Current Owner 2'),
        ('col', 'Deed Book/Page'),
        ('col', 'Current Owner Address'),
        ('col', 'Date Recorded'),
        ('col', 'Deferred Amount'),
        ('col', 'Building Value'),
        ('col', 'Tax Codes'),
        ('col', 'Land Value'),
        ('col', 'Township Codes'),
        ('col', 'Subdivision Codes'),
        ('col', 'Account Number'),
        ('col', 'Map Sheet'),
        ('col', 'Sale Price'),
        ('col', 'Total Tax Value'),
    ]
    params = {
        "DEST": "download",
        "action": "Search",
    }

    all_records = []
    page_num = 1

    while True:
        print(f"Scraping page: {page_num}")
        # Add/update page number in payload for each request
        payload = base_payload + [('page_no', page_num)]

        try:
            # Use POST since we are sending data in the payload
            r = requests.post(url=url, headers=headers, params=params, data=payload)
            r.raise_for_status()  # Raise an exception for bad status codes
            soup = BeautifulSoup(r.text, 'html.parser')

            # Find the correct data table
            tables = soup.find_all("table")
            if len(tables) < 7:
                print("Could not find the data table on page {page_num}.")
                break
            
            data_table = tables[6]
            
            # Find all table rows, skipping the header row
            rows = data_table.find_all('tr')[1:]

            # If there are no rows, we've reached the last page
            if not rows:
                print("No more data found. Assuming last page reached.")
                break

            for row in rows:
                tds = row.find_all('td')
                if len(tds) < 18:
                    continue # Skip malformed rows

                record = {
                    'parcelID': tds[0].text.strip(),
                    'property_address': tds[1].text.strip(),
                    'property_description': tds[2].text.strip(),
                    'current_owner1': tds[3].text.strip(),
                    'current_owner2': tds[4].text.strip(),
                    'deed_book_page': tds[5].text.strip(),
                    'current_owner_address': tds[6].text.strip(),
                    'date_recorded': tds[7].text.strip(),
                    'deferred_amount': tds[8].text.strip(),
                    'building_value': tds[9].text.strip(),
                    'tax_code': tds[10].text.strip(),
                    'land_value': tds[11].text.strip(),
                    'township_codes': tds[12].text.strip(),
                    'subdivision_codes': tds[13].text.strip(),
                    'account_number': tds[14].text.strip(),
                    'map_sheet': tds[15].text.strip(),
                    'sale_price': tds[16].text.strip(),
                    'total_tax_value': tds[17].text.strip(),
                }
                all_records.append(record)

            page_num += 1
            # Add a small delay to be respectful to the server
            time.sleep(random.uniform(0.5, 1.5))

        except requests.exceptions.RequestException as e:
            print(f"An error occurred during the request: {e}")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            break

    # Create a DataFrame from the collected records
    if all_records:
        return pd.DataFrame(all_records)
    else:
        return pd.DataFrame()

if __name__ == '__main__':
    df = scrape_edgecombe_parcels()
    if not df.empty:
        print("\n--- Scraped Data ---")
        print(df)
        df.to_csv('edgecombe_parcels.csv', index=False)
        print(f"\nSuccessfully scraped {len(df)} records.")
    else:
        print("No data was scraped.")