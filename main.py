import time
import re
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from pprint import pprint

# This script scrapes all parcel data from the Edgecombe County tax website,
# saves it to a CSV file, and displays it in a Pandas DataFrame.

def scrape_and_save_edgecombe_parcels():
    """
    Scrapes all parcel data, saves it to a CSV, and returns a DataFrame.
    """
    url = "https://taxpa.edgecombecountync.gov/paas/export.asp"
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'referer': 'https://taxpa.edgecombecountync.gov/paas/?DEST=download&action=Export',
    }
    # Base payload for the search
    base_payload = [
        ('TAX', 'G01'),
        ('col', 'Parcel ID'),
        ('col', 'Property Address'),
        ('col', 'Property Description'),
        ('col', 'Current Owner 1'),
        ('col', 'Current Owner 2'),
        ('col', 'Deed Book/Page'),
        ('col', 'Current Owner Address'),
        ('col', 'Date Recorded'),
        ('col', 'Building Value'),
        ('col', 'Land Value'),
        ('col', 'Total Tax Value'),
    ]

    all_records = []
    page_num = 1

    while True:
        print(f"Scraping page: {page_num}")
        # Add/update page number in payload for each request
        payload = base_payload + [('page_no', page_num)]

        try:
            r = requests.post(url=url, headers=headers, data=payload)
            r.raise_for_status()  # Raise an exception for bad status codes
            soup = BeautifulSoup(r.text, 'html.parser')

            # Find the correct data table
            tables = soup.find_all("table")
            if not tables:
                print(f"No table found on page {page_num}.")
                break
            
            data_table = tables[0]
            
            # Find all table rows, skipping the header row
            rows = data_table.find_all('tr')[1:]

            # If there are no rows, we've reached the last page
            if not rows:
                print("No more data found. Assuming last page reached.")
                break

            for row in rows:
                tds = row.find_all('td')
                if len(tds) < len(base_payload) -1:
                    continue # Skip malformed rows

                record = {
                    'Parcel ID': tds[0].text.strip(),
                    'Property Address': tds[1].text.strip(),
                    'Property Description': tds[2].text.strip(),
                    'Current Owner 1': tds[3].text.strip(),
                    'Current Owner 2': tds[4].text.strip(),
                    'Deed Book/Page': tds[5].text.strip(),
                    'Current Owner Address': tds[6].text.strip(),
                    'Date Recorded': tds[7].text.strip(),
                    'Building Value': tds[8].text.strip(),
                    'Land Value': tds[9].text.strip(),
                    'Total Tax Value': tds[10].text.strip(),
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

    if all_records:
        df = pd.DataFrame(all_records)
        # Save to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"edgecombe_parcels_{timestamp}.csv"
        df.to_csv(csv_filename, index=False)
        print(f"\nData saved to {csv_filename}")
        return df
    else:
        return pd.DataFrame()

if __name__ == '__main__':
    df = scrape_and_save_edgecombe_parcels()
    if not df.empty:
        print("\n--- Scraped Data ---")
        print(df)
        df.to_csv('edgecombe_parcels_data.csv', index=False)
        print(f"\nSuccessfully scraped {len(df)} records.")
    else:
        print("No data was scraped.")