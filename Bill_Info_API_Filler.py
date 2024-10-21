# -*- coding: utf-8 -*-
"""
Created on Sun Oct 20 11:08:48 2024

@author: dforc

## Description:

    This script processes bill information from a CSV file by retrieving additional 
    data from the Congress API using multiple API keys. It handles rate limits 
    through key rotation and retries, processes data in batches, and saves the final 
    output into a CSV file. The script also logs errors during execution for review. 
    The key steps include loading bill data, rotating through API keys, fetching data 
    asynchronously, and saving the results in batches.

### Segment Overview:

    1. **Segment 1: Load and clean bill_info.csv**  

    2. **Segment 2: Split data into batches**  

    3. **Segment 3: Load API keys and create asyncio queues for each group**  
        - Loads API keys from a JSON file and places them in asynchronous queues. The 
          queues allow for rotating between API keys to avoid hitting rate limits.

    4. **Segment 4: Retrieve API key from queue for a specific endpoint group**  
        - Retrieves an API key from the appropriate queue for a specific API endpoint. 
          The API key is returned to the queue after use, ensuring rotation.

    5. **Segment 5: Fetch data from an API endpoint**  
        - Fetches data from the Congress API for a given endpoint and returns the response, 
          handling errors and retrying if necessary.

    6. **Segment 6: Fetch data for a specific bill from an endpoint**  
        - Fetches data for a specific bill record from the appropriate API endpoint. 
          It manages retries and handles rate limit errors.

    7. **Segment 7: Fetch data for all endpoints for a single bill record**  
        - Gathers data from all relevant API endpoints for a single bill record, using 
          asynchronous tasks to make multiple requests in parallel.

    8. **Segment 8: Process batch of records and fetch data**  
        - Processes a batch of bill records, fetching data for each record from multiple 
          API endpoints. It combines the fetched data with the original bill record and 
          appends the results for later saving.

    9. **Segment 9: Process all batches and save results to CSV**  
        - Orchestrates the processing of all batches. It handles batch processing sequentially, 
          saving the output after each batch to avoid memory overload. It also tracks processing time.

    10. **Segment 10: Main entry point to run the script**  
        - This is the main function that ties everything together. It loads the bill data, 
          API keys, checks for already processed records, splits data into batches, and runs 
          the asynchronous processing and saving functions.
"""


import pandas as pd
import json
import aiohttp
import asyncio
import time
import os
import logging
from tqdm import tqdm  ## For progress bars
import nest_asyncio

nest_asyncio.apply()

########################################################################
### Segment 1: Load and clean bill_info.csv
########################################################################

def load_and_index_bills(file_path):
    """
    Load and clean the bill information from a CSV file, then add an index column.
    """
    
    ## Read .csv containing Bill Number, Congress Number, and Bill Type
    df = pd.read_csv(file_path, low_memory=False)

    ## Drop rows with NA in 'congress', 'number', or 'type' columns
    df = df.dropna(subset=['congress', 'number', 'type'])

    ## Convert 'number' to numeric, coercing errors to NaN, then drop NaN values
    df['number'] = pd.to_numeric(df['number'], errors='coerce')
    df = df.dropna(subset=['number'])

    ## Ensure 'congress' and 'number' are integers
    df['congress'] = df['congress'].astype(int)
    df['number'] = df['number'].astype(int)

    ## Convert 'type' to lowercase strings
    df['type'] = df['type'].str.lower()

    ## Reset index and store it in a column
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'index'}, inplace=True)

    return df



########################################################################
### Segment 2: Split data into batches for parallel processing
########################################################################

def split_into_batches(df, batch_size):
    """
    Split the DataFrame into smaller batches.
    """
    return [df[i:i + batch_size] for i in range(0, len(df), batch_size)]



########################################################################
### Segment 3: Load API keys and create asyncio queues for each group
########################################################################

def load_api_key_queues(env_file):
    """
    Load API keys from a JSON-formatted file and create an asyncio.Queue for each group.
    """
    with open(env_file, 'r') as file:
        api_keys = json.load(file)

    api_key_queues = {}
    ## Loop through each group of API keys and add them to a queue
    for group_name, keys in api_keys.items():
        queue = asyncio.Queue()
        for key in keys:
            queue.put_nowait(key)
        api_key_queues[group_name] = queue

    return api_key_queues



########################################################################
### Segment 4: Retrieve API key from queue for a specific endpoint group
########################################################################

async def get_api_key(api_key_queues, endpoint_group):
    """
    Retrieve an API key from the queue for a specific endpoint group.
    """
    queue = api_key_queues[endpoint_group]
    api_key = await queue.get()
    await queue.put(api_key)  ## Return the key to the queue for future use
    return api_key



########################################################################
### Segment 5: Fetch data from an API endpoint
########################################################################

async def fetch_data(session, url):
    """
    Fetch data from a given URL using an aiohttp session.
    """
    try:
        async with session.get(url) as response:
            status = response.status
            if status == 200:
                ## Successfully fetched data
                data = await response.json()
                return data, status
            else:
                return None, status
    except Exception:
        ## Handle exception during fetching
        return None, -1
    
    

########################################################################
### Segment 6: Fetch data for a specific bill from an endpoint
########################################################################

async def fetch_endpoint_data(session, semaphore, api_key_queues, api_key_group,
                              congress, bill_type, bill_number, endpoint, endpoint_name, shared_state):
    """
    Fetch data from a specific endpoint for a bill, using a semaphore to limit concurrency.
    """
    async with semaphore:
        max_retries = shared_state['max_retries_per_request']
        retry_count = 0
        retry_statuses = shared_state.get('retry_statuses', {429})  ## Retry on rate limit errors

        ## Retry loop
        while retry_count < max_retries:
            retry_needed = False
            api_keys_in_group = len(api_key_queues[api_key_group]._queue)
            for _ in range(api_keys_in_group):
                ## Get an API key and construct the URL
                api_key = await get_api_key(api_key_queues, api_key_group)
                url = f'https://api.congress.gov/v3/bill/{congress}/{bill_type}/{bill_number}{endpoint}?api_key={api_key}'

                ## Fetch data
                data, status = await fetch_data(session, url)

                ## Handle different statuses and retry logic
                if status in retry_statuses:
                    retry_needed = True
                    async with shared_state['error_lock']:
                        shared_state['error_count'] += 1
                    logging.error(f"Rate limit exceeded (status {status}). Retrying with another key.")
                    continue
                elif data is None:
                    if status == -1:
                        async with shared_state['error_lock']:
                            shared_state['error_count'] += 1
                        logging.error(f"Exception fetching data for Congress {congress}, Bill {bill_type} {bill_number}, Endpoint '{endpoint_name}'.")
                    else:
                        async with shared_state['error_lock']:
                            shared_state['error_count'] += 1
                        logging.error(f"HTTP error {status} fetching data for Congress {congress}, Bill {bill_type} {bill_number}, Endpoint '{endpoint_name}'.")
                    break
                else:
                    return endpoint_name, data

            if retry_needed:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = shared_state['backoff_factor'] * retry_count
                    logging.error(f"All API keys rate limited. Waiting {wait_time} seconds before retrying...")
                    await asyncio.sleep(wait_time)
                else:
                    async with shared_state['error_lock']:
                        shared_state['error_count'] += 1
                    logging.error(f"Max retries reached for Congress {congress}, Bill {bill_type} {bill_number}. Skipping.")
                    break
            else:
                break

    return endpoint_name, data



########################################################################
### Segment 7: Fetch data for all endpoints for a single bill record
########################################################################

async def fetch_record_data(session, semaphore, api_key_queues, record, shared_state):
    """
    Fetch data from all endpoints for a single bill record.
    """
    congress = record['congress']
    bill_type = record['type']
    bill_number = record['number']

    ## Define endpoints to query
    endpoints = [
        '',  # Base endpoint (Sponsors)
        '/cosponsors',
        '/actions',
        '/amendments',
        '/committees',
        '/subjects',
        '/relatedbills',
        '/summaries'
    ]

    results = {}
    tasks = []

    ## Create asyncio tasks for each endpoint
    for i, endpoint in enumerate(endpoints):
        api_key_group = f"group_{i + 1}"
        endpoint_name = endpoint.lstrip('/') if endpoint else 'sponsors'
        task = asyncio.create_task(
            fetch_endpoint_data(
                session, semaphore, api_key_queues, api_key_group,
                congress, bill_type, bill_number,
                endpoint, endpoint_name, shared_state
            )
        )
        tasks.append(task)

    ## Wait for all endpoint data fetches to complete
    endpoint_results = await asyncio.gather(*tasks)

    ## Aggregate results
    for endpoint_name, data in endpoint_results:
        results[endpoint_name] = data

    return results



########################################################################
### Segment 8: Process batch of records and fetch data
########################################################################

async def process_batch(session, semaphore, api_key_queues, batch, output_rows, shared_state):
    """
    Process a batch of bill records.
    """
    ## Progress bar for batch processing
    pbar = tqdm(total=len(batch), desc='Processing Records', leave=False)

    async def process_record(record):
        try:
            result = await fetch_record_data(session, semaphore, api_key_queues, record.to_dict(), shared_state)
        except Exception as e:
            ## Handle exceptions and log them
            async with shared_state['error_lock']:
                shared_state['error_count'] += 1
            logging.error(f"Exception processing record {record['index']}: {str(e)}")
            result = None
        pbar.update(1)
        return result

    ## Create tasks for each record in the batch
    tasks = [process_record(record) for _, record in batch.iterrows()]

    ## Wait for all tasks to complete
    results = await asyncio.gather(*tasks)

    pbar.close()

    ## Combine original records with fetched data
    for result, record in zip(results, batch.to_dict(orient='records')):
        if result is not None:
            combined_record = {**record, **result}
            output_rows.append(combined_record)
            
            

########################################################################
### Segment 9: Main function to process batches and save results
########################################################################

async def process_batches(api_key_queues, batches, output_file):
    """
    Process all batches and save the results to a CSV file.
    """
    start_time = time.time()
    output_rows = []

    ## Shared state for error tracking and retry limits
    shared_state = {
        'error_count': 0,
        'error_lock': asyncio.Lock(),
        'max_retries_per_request': 3,
        'backoff_factor': 5,
        'retry_statuses': {429}
    }

    ## Create shared session and semaphore
    async with aiohttp.ClientSession() as session:
        max_concurrent_requests = 100  ## Adjust based on system/API limits
        semaphore = asyncio.Semaphore(max_concurrent_requests)

        ## Process each batch
        for batch in tqdm(batches, desc='Processing Batches'):
            await process_batch(session, semaphore, api_key_queues, batch, output_rows, shared_state)

            ## Save output to CSV
            output_df = pd.DataFrame(output_rows)
            output_df.to_csv(output_file, index=False, mode='a', header=not os.path.exists(output_file))
            output_rows.clear()

    end_time = time.time()
    print(f"\n****** Script completed in {end_time - start_time:.2f} seconds ******")
    
    

########################################################################
### Segment 10: Main entry point to run the script
########################################################################

def main():
    """
    Main function to orchestrate the loading of data, processing, and saving results.
    """
    ## Configure logging
    logging.basicConfig(filename='errors.log', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')

    ## Define file paths
    bill_file = 'bill_info.csv'
    env_file = 'cong_api_keys.env'
    output_file = 'bill_info_filled.csv'

    ## Load and preprocess bill data
    bill_data = load_and_index_bills(bill_file)

    ## Load API keys
    api_key_queues = load_api_key_queues(env_file)

    ## Check if output file exists
    if os.path.exists(output_file):
        processed_data = pd.read_csv(output_file)
        processed_indices = set(processed_data['index'])
        bill_data = bill_data[~bill_data['index'].isin(processed_indices)]

    ## Process batches if data remains
    if not bill_data.empty:
        batches = split_into_batches(bill_data, 16)
        asyncio.run(process_batches(api_key_queues, batches, output_file))

## Entry point
if __name__ == '__main__':
    main()


