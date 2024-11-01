import pandas as pd
from vnstock3 import Vnstock
import time
import json
import os

# Initialize Vnstock instance and fetch stock data
vnstock_instance = Vnstock()

# Set display options to show all rows and columns
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Get the list of all stock symbols and fetch industry data
stock = vnstock_instance.stock(symbol='FPT', source='VCI')
all_symbols = stock.listing.all_symbols().head(50)  # Fetch all stock symbols
industry_data = stock.listing.symbols_by_industries()  # Fetch industry data

# Convert industry data to a dictionary for easy lookup
industry_dict = industry_data.set_index('symbol').to_dict(orient='index')

# File path for the JSON file
file_path = "stock_data_with_industries.json"

# Open the JSON file for writing and initialize it as a JSON array
with open(file_path, "w", encoding="utf-8") as f:
    f.write("[\n")

# Iterate over each ticker and fetch the history
first_record = True  # To keep track of the first record for proper comma handling
for index, row in all_symbols.iterrows():
    ticker = row['ticker']

    # Retrieve industry details or use blank values if not found
    industry_info = industry_dict.get(ticker, {})
    organ_name = industry_info.get('organ_name', '')
    en_organ_name = industry_info.get('en_organ_name', '')
    icb_name3 = industry_info.get('icb_name3', '')
    en_icb_name3 = industry_info.get('en_icb_name3', '')
    icb_name2 = industry_info.get('icb_name2', '')
    en_icb_name2 = industry_info.get('en_icb_name2', '')
    icb_name4 = industry_info.get('icb_name4', '')
    en_icb_name4 = industry_info.get('en_icb_name4', '')
    com_type_code = industry_info.get('com_type_code', '')
    icb_code1 = industry_info.get('icb_code1', '')
    icb_code2 = industry_info.get('icb_code2', '')
    icb_code3 = industry_info.get('icb_code3', '')
    icb_code4 = industry_info.get('icb_code4', '')

    try:
        history = stock.quote.history(symbol=ticker, start='2003-1-1', end='2024-10-30', interval='1D')
        
        # Add all fields to the history DataFrame
        history['ticker'] = ticker
        history['organ_name'] = organ_name
        history['en_organ_name'] = en_organ_name
        history['icb_name3'] = icb_name3
        history['en_icb_name3'] = en_icb_name3
        history['icb_name2'] = icb_name2
        history['en_icb_name2'] = en_icb_name2
        history['icb_name4'] = icb_name4
        history['en_icb_name4'] = en_icb_name4
        history['com_type_code'] = com_type_code
        history['icb_code1'] = icb_code1
        history['icb_code2'] = icb_code2
        history['icb_code3'] = icb_code3
        history['icb_code4'] = icb_code4

        # Convert Timestamp columns to strings
        history = history.applymap(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) else x)

        # Convert the DataFrame to a dictionary
        stock_data = history.to_dict(orient='records')

        # Open the JSON file in append mode
        with open(file_path, "a", encoding="utf-8") as f:
            if not first_record:
                f.write(",\n")  # Add a comma and newline before each new record
            else:
                first_record = False  # Skip comma for the first record

            # Write the current stock data as JSON
            json.dump(stock_data, f, ensure_ascii=False, indent=4)

        print(f"Data for {ticker} successfully written to {file_path}")
    except ValueError as e:
        print(f"Error fetching data for ticker {ticker}: {e}")

    time.sleep(1)

# Close the JSON array in the file
with open(file_path, "a", encoding="utf-8") as f:
    f.write("\n]")
