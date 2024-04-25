'''
dreams labs functions that related to dune api interactions. 

IMPORTANT: much of this code can be done more simply with the dune python sdk (see
https://docs.dune.com/api-reference/overview/sdks for more information). for the standard process
of running sql and retrieving results in a dataframe, it is recommended to use the dune sdk 
function dune.run_query_dataframe() rather than multiple functions within this package. 
'''

import io
import json
import requests
import pandas as pd


def trigger_query(
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


def check_query_status(
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


def get_query_results(
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
