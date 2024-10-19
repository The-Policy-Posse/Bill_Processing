"""
Congress_API.py

This script bypasses the return limit set on the Congress API call by recursively switching
between date range granularities and continuing from the previous granularity when finished.

Range granularity values for each request have been added in the dataframe in order to not have 
to spend API calls to drill down by date ranges for future requests. This should be useful for 
getting summary data for each bill. 

All data from the database will be written to a .csv file in the working directory. 

Params:
- API key
- API request url
- Date ranges

Returns:
- .csv file

Author: Ryan Hopkins
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import time

# Load environment variables from a .env file
load_dotenv('creds.env')

def generate_date_range(start_date, end_date, increment='month'):
    """
    Yields dates dates within a range based on the specified increment.
    
    Args:
    start_date (datetime): Start of the date range.
    end_date (datetime): End of the date range.
    increment (str): Time increment ('month', 'week', 'day', '4hour', 'hour', '15min', '3min').
    
    Yields:
    datetime: Next date in the sequence.
    """
    # Generates dates within a range based on the specified increment
    current_date = start_date
    while current_date < end_date:
        yield current_date # Yield allows the function to continue previous granularity date range
        if increment == 'month':
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
            else:
                current_date = current_date.replace(month=current_date.month % 12 + 1, day=1)
        elif increment == 'week':
            current_date += timedelta(weeks=1)
        elif increment == 'day':
            current_date += timedelta(days=1)
        elif increment == '4hour':
            current_date += timedelta(hours=4)
        elif increment == 'hour':
            current_date += timedelta(hours=1)
        elif increment == '15min':
            current_date += timedelta(minutes=15)
        elif increment == '3min':
            current_date += timedelta(minutes=3)
        current_date = min(current_date, end_date)


def congress_api_call(start_date, end_date, api_key, limit=250):
    """
    Makes a single API call to Congress.gov for bills within a date range.
    
    Args:
    start_date (datetime): Start of the query range.
    end_date (datetime): End of the query range.
    api_key (str): API key for Congress.gov.
    limit (int): Maximum number of results to return.
    
    Returns:
    DataFrame: Bills data with query time range.
    """
    # Makes a single API call to Congress.gov
    bill_url = f'https://api.congress.gov/v3/bill?fromDateTime={start_date.strftime("%Y-%m-%dT%H:%M:00Z")}&toDateTime={end_date.strftime("%Y-%m-%dT%H:%M:00Z")}&limit={limit}&api_key={api_key}'
    bill_pull = requests.get(bill_url).json()
    df = pd.DataFrame(bill_pull.get('bills', []))
    if not df.empty:
        df['query_start_time'] = start_date
        df['query_end_time'] = end_date
    
    # Rate limiter
    time.sleep(.75)

    return df


def adaptive_api_call(start_date, end_date, api_key, response_limit=250):
    """
    Adaptively calls the Congress.gov API based on data density.
    
    Args:
    start_date (datetime): Overall start date for data collection.
    end_date (datetime): Overall end date for data collection.
    api_key (str): API key for Congress.gov.
    response_limit (int): Threshold to trigger finer granularity.
    
    Returns:
    DataFrame: Compiled bill data across all granularities.
    """
    # Main function to adaptively call the API based on data density
    granularity_levels = ['month', 'week', 'day', '4hour', 'hour', '15min', '3min']

    def recursive_call(start, end, granularity_index, indent=""):
        granularity = granularity_levels[granularity_index]
        date_range = list(generate_date_range(start, end, granularity))
        all_data = pd.DataFrame()
        
        for i, date in enumerate(date_range):
            # Determine the end date for this iteration
            if i + 1 < len(date_range):
                next_date = date_range[i + 1]
            else:
                next_date = end
            
            # Make the API call
            data = congress_api_call(date, next_date, api_key, limit=response_limit)
            
            # Log the results
            if granularity in ['4hour', 'hour', '15min', '3min']:
                print(f"{indent}{granularity.capitalize()} {date.strftime('%Y-%m-%d %H:%M')}: {len(data)} bills")
            else:
                print(f"{indent}{granularity.capitalize()} {date.strftime('%Y-%m-%d')}: {len(data)} bills")
            
            # If limit reached and finer granularity available, drill down
            if len(data) >= response_limit and granularity_index < len(granularity_levels) - 1:
                print(f"{indent}Limit reached, drilling down...")
                finer_data = recursive_call(date, next_date, granularity_index + 1, indent + "  ")
                all_data = pd.concat([all_data, finer_data], ignore_index=True)
            else:
                all_data = pd.concat([all_data, data], ignore_index=True)
        
        # Log total bills for this granularity
        print(f"{indent}Total for {granularity}: {len(all_data)} bills")
        return all_data

    return recursive_call(start_date, end_date, 0)


def retrieve_data(start_date, end_date, api_key):
    """
    Retrieves all data returned by Congress.gov API, using date parameters and bypassing limit parameters.
    Writes data as a csv to working directory.
    
    Args:
    start_date (datetime): Overall start date for data collection.
    end_date (datetime): Overall end date for data collection.
    api_key (str): API key for Congress.gov.
    
    Returns:
    DataFrame: Compiled bill data across all granularities.
    """

    print("Starting adaptive API call process...")
    bill_df = adaptive_api_call(start_date, end_date, api_key)

    print(f'\nTotal bills retrieved: {bill_df.shape[0]}')

    # Write the DataFrame to CSV
    bill_filename = 'adaptive_bill_info.csv'
    print('Writing csv...')
    bill_df.to_csv(bill_filename, index=False)
    print(f"File saved. Location: {os.path.abspath(bill_filename)}")


def main():
    api_key = os.getenv('CONGRESS_API_KEY')
    print(api_key)
    start_date = datetime(2016, 10, 26)
    end_date = datetime(2024, 10, 27)
    retrieve_data(start_date, end_date, api_key)
    return None

if __name__ == "__main__":
    main()