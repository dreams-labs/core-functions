import datetime
import time
import math
import os
import io
import json
import pdb
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import google.auth
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import secretmanager_v1
from google.oauth2 import service_account


def human_format(num):
    '''
    converts a number to a scaled human readable string (e.g 7437283-->7.4M)

    TODO: the num<1 code should technically round upwards when truncating the
    string, e.g. 0.0678 right now will display as 0.067 but should be 0.068

    param: num <numeric>: the number to be reformatted
    return: formatted_number <string>: the number formatted as a human-readable string
    '''
    if num < 1:
        # decimals are output with enough precision to show two non-0 numbers
        num = np.format_float_positional(num, trim='-')
        after_decimal = str(num[2:])
        keep = 4+len(after_decimal) - len(after_decimal.lstrip('0'))
        num = num[:keep]
    else:
        num = float('{:.3g}'.format(num))
        magnitude = 0
        while abs(num) >= 1000:
            magnitude += 1
            num /= 1000.0
        # copy pasted from github and is very difficult to understand as written
        num='{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['','k','m','B','T','QA','QI','SX','SP','O','N','D'][magnitude])

    return num


def get_secret(
        secret_name,
        service_account_path=None,
        project_id='954736581165',
        version='latest'
    ):
    '''
    Retrieves a secret from GCP Secrets Manager.

    Parameters:
    secret_name (str): The name of the secret in Secrets Manager.
    service_account_path (str, optional): Path to the service account JSON file.
    version (str): The version of the secret to be loaded.

    Returns:
    str: The value of the secret.
    '''
    # Construct the resource name of the secret version.
    secret_path = f'projects/{project_id}/secrets/{secret_name}/versions/{version}'

    # Initialize the Google Secret Manager client
    client = initialize_secret_manager_client(service_account_path)

    # Request to access the secret version
    request = secretmanager_v1.AccessSecretVersionRequest(name=secret_path)
    response = client.access_secret_version(request=request)
    return response.payload.data.decode('UTF-8')


def initialize_secret_manager_client(service_account_path):
    '''
    Initialize the Secret Manager client with the appropriate credentials.

    Parameters:
    service_account_path (str): Path to the service account JSON file.

    Returns:
    SecretManagerServiceClient: A client for the Secret Manager Service.
    '''
    if service_account_path:
        # Explicitly use the provided service account file for credentials
        credentials = service_account.Credentials.from_service_account_file(service_account_path)
    else:
        # Attempt to use default credentials
        credentials, _ = google.auth.default()

    return secretmanager_v1.SecretManagerServiceClient(credentials=credentials)

# Example usage
if __name__ == "__main__":
    load_dotenv()  # Load environment variables from .env file at the start of your application
    secret = get_secret("apikey_coingecko_tentabs_free")
    print(secret)


### DUNE INTERACTIONS ###
def dune_trigger_query(
        dune_api_key,
        query_id,
        query_parameters,
        query_engine='medium',
        verbose=False
    ):
    """
    Runs a Dune query via API based on the input query ID and parameters.

    Parameters:
        dune_api_key (str): The Dune API key.
        query_id (int): Dune's query ID (visible in the URLs).
        query_parameters (dict): The query parameters to input to the Dune query.
        query_engine (str): The Dune query engine type to use (options are 'medium' or 'large').
        verbose (bool): If True, prints detailed debug information.

    Returns:
        int: The query execution ID or None if the query fails.

    Raises:
        RequestException: If an error occurs during the API request.
    """
    headers = {'X-DUNE-API-KEY': dune_api_key}
    base_url = f'https://api.dune.com/api/v1/query/{query_id}/execute'
    params = {
        'query_parameters': query_parameters,
        'performance': query_engine,
    }

    try:
        response = requests.post(base_url, headers=headers, json=params, timeout=30)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        response_data = response.json()

        execution_id = response_data.get("execution_id")
        if verbose:
            print(f'Dune query triggered successfully, execution ID: {execution_id}')

        return execution_id
    except requests.exceptions.RequestException as e:
        if verbose:
            print(f'Dune query trigger failed: {str(e)}')
        raise  # Optionally re-raise exception after logging

    return None


def dune_check_query_status(
        dune_api_key,
        execution_id,
        verbose=False
    ):
    '''
    checks the status of a dune query. possible statuses include:
    QUERY_STATE_QUEUED
    QUERY_STATE_PENDING
    QUERY_STATE_EXECUTING
    QUERY_STATE_COMPLETED
    QUERY_STATE_FAILED

    param: dune_api_key <string> the dune API key
    param: execution_id <int> the query execution ID
    
    return: query_status <string> the status of the query
    '''
    headers = {"X-DUNE-API-KEY": dune_api_key}
    url = "https://api.dune.com/api/v1/execution/"+str(execution_id)+"/status"

    response = requests.request("GET", url, headers=headers, timeout=30)
    response_data = json.loads(response.text)

    if 'error' in response_data:
        query_status = 'QUERY_STATE_FAILED'

    else:
        # QUERY_STATE_COMPLETED
        query_status = response_data["state"]

    if verbose:
        print(query_status)

    return query_status


def dune_get_query_results(
        dune_api_key,
        execution_id
    ):
    '''
    retrieves the results from a dune query attempt

    param: dune_api_key <string> the dune API key
    param: execution_id <int> the query execution ID

    return: api_status_code <int> the api response of the dune query
    return: query_df <dataframe> the dataframe of results if valid
    '''

    # retreive the results
    headers = {"X-DUNE-API-KEY": dune_api_key}
    url = "https://api.dune.com/api/v1/execution/"+str(execution_id)+"/results/csv"
    response = requests.request("GET", url, headers=headers, timeout=30)

    if response.status_code == 200:
        query_df = pd.read_csv(io.StringIO(response.text), index_col=0)
        query_df = query_df.reset_index()
    else:
        query_df = None

    return(response.status_code,query_df)
