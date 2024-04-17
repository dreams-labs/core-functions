import datetime
import time
import math
import os
import io
import json
import pdb
from dotenv import load_dotenv
import requests
import pandas as pd
import numpy as np
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
      secret_name
      ,version='latest'
    ):
    '''
    retrieves a secret. works within bigquery python notebooks and needs
    testing in cloud functions

    param: secret_name <string> the name of the secret in secrets manager, 
        e.g. "apikey_coingecko_tentabs_free"
    param: version <string> the version of the secret to be loaded (only valid for notebooks)
    return: secret_value <string> the value of the secret
    '''
    project_id = '954736581165' # dreams labs project id (western-verve-411004)
    secret_path=f'projects/{project_id}/secrets/{secret_name}/versions/{version}'

    try:
        # load credentials from environmental variables
        load_dotenv()
        service_account_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

        if service_account_path:
            # if path is found use it for credentials (works in vscode)
            credentials = service_account.Credentials.from_service_account_file(service_account_path)
            client = secretmanager_v1.SecretManagerServiceClient(credentials=credentials)
        else:
            # if path isn't found, use default env variables (works in bigquery notebooks)
            client = secretmanager_v1.SecretManagerServiceClient()

        # initiate client and request secret
        request = secretmanager_v1.AccessSecretVersionRequest(name=secret_path)
        response = client.access_secret_version(request=request)
        secret_value = response.payload.data.decode('UTF-8')
    except:
        # syntax that works in GCF
        secret_value = os.environ.get(secret_name)

    return secret_value


def bigquery_run_sql(
        query_sql
        ,service_account_secret='service_account_eng_general'
        ,location='US'
        ,project = 'western-verve-411004'
    ):
    '''
    runs a query and returns results as a dataframe. the service account credentials to 
    grant access are autofilled to a general service account. there are likely security
    optimizations that will need to be done in the future. 

    param: query_sql <string> the query to run
    param: service_account_secret <json> the name of the GCP secrets manager secret that \
        contains the service account jeson data
    param: location <string> the location of the bigquery project
    param: project <string> the project ID of the bigquery project
    return: query_df <dataframe> the query result
    '''
    # prepare credentials using a service account stored in GCP secrets manager
    service_account_info = json.loads(get_secret(service_account_secret))
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info
        ,scopes=scopes
    )

    # create a bigquery client object and run the query
    client = bigquery.Client(project=project,location=location,credentials=credentials)
    query_job = client.query(query_sql)
    query_df = query_job.to_dataframe()

    return query_df
