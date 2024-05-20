'''
common functions for interacting with BigQuery within the dreams labs data ecosystem. all
functions are initiated through the BigQuery() class which contains credentials, project ids, and
other relevant metadata. 
'''

import datetime
import os
import logging
import json
import pandas as pd
import google.auth
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage

class GoogleCloud:
    ''' 
    A class to interact with BigQuery. This class is designed
    to be used in the context of the Dreams project. It is not
    intended to be a general purpose BigQuery class.

    Params: 
        service_account_json (str): Path to the service account JSON file. if no value is input \
        the functions will default to using the path in the env var GOOGLE_APPLICATION_CREDENTIALS
    '''

    def __init__(
            self,
            service_account_json_path=None
        ):
        # load credentials using service account and scope
        if service_account_json_path is None:
            service_account_json_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                service_account_json_path
                ,scopes=scopes
            )
        except Exception as e: 
            self.credentials, _ = google.auth.default()

        # other variables
        self.location = 'US'
        self.project_id = 'western-verve-411004'
        self.project_name = 'dreams-labs-data'
        self.bucket_name = 'dreams-labs-storage'

        # configure logger
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(logging.NullHandler())


    def run_sql(
            self,
            query_sql
        ):
        '''
        runs a query and returns results as a dataframe. the service account credentials to 
        grant access are autofilled to a general service account. there are likely security
        optimizations that will need to be done in the future. 

        param: query_sql <string> the query to run
        param: location <string> the location of the bigquery project
        param: project <string> the project ID of the bigquery project
        return: query_df <dataframe> the query result
        '''
        # prepare credentials using a service account stored in GCP secrets manager


        # create a bigquery client object and run the query
        client = bigquery.Client(
            project=self.project_id,
            location=self.location,
            credentials=self.credentials
        )
        query_job = client.query(query_sql)
        query_df = query_job.to_dataframe()

        self.logger.info('BigQuery query completed.')

        return query_df


    def cache_sql(
            self,
            query_sql,
            cache_file_name,
            freshness=24,
        ):
        '''
        tries to use a cached result of a query from gcs. if it doesn't exist or
        is stale, reruns the query and returns fresh results.

        cache location: dreams-labs-storage/cache

        param: query_sql <string> the query to run
        param: cache_file_name <string> what to name the cache
        param: freshness <float> how many hours before a refresh is required
        return query_df <dataframe> the cached or fresh result of the query
        '''

        filepath = f'cache/query_{cache_file_name}.csv'
        client = storage.Client(project=self.project_name, credentials=self.credentials)
        bucket = client.get_bucket(self.bucket_name)

        # Attempt to retrieve file freshness
        try:
            blob = bucket.get_blob(filepath)
            file_freshness = blob.updated if blob else None
        except Exception as e:
            print(f"error retrieving blob: {e}")
            file_freshness = None

        # Determine cache staleness
        cache_stale = (
            file_freshness is None or
            (
                (datetime.datetime.now(tz=datetime.timezone.utc) - file_freshness)
                .total_seconds() / 3600 > freshness
            )
        )
        # Refresh cache if stale
        if cache_stale:
            query_df = self.run_sql(query_sql)
            blob = bucket.blob(filepath)
            blob.upload_from_string(query_df.to_csv(index=False), content_type='text/csv')

            self.logger.info('returned fresh csv and refreshed cache')
        else:
            query_df = pd.read_csv(f'gs://{self.bucket_name}/{filepath}')

            self.logger.info('returned cached csv')

        return query_df


    def gcs_upload_file(
            self,
            data,
            gcs_folder,
            filename,
            project_name='dreams-labs-data',
            bucket_name='dreams-labs-storage',
        ):
        '''
        uploads a file to google cloud storage. currently accepted input formats are \
        dataframes or dicts. 

        Params:
            data: <dict> or <dataframe> the data to upload
            gcs_folder: <string> the upload folder in gcs, e.g. 'data_lake/coingecko_market_data'
            filename: <string> the name the gcs file will be given, e.g. 'aioz-network.json'
            project_name: <string> google cloud project name
            bucket_name: <string> GCS bucket name
        '''

        # adjust filename to append filetype if one isn't included
        if '.' in filename:
            pass
        elif isinstance(data, pd.DataFrame):
            filename = f'{filename}.csv'
        elif isinstance(data, dict):
            filename = f'{filename}.json'
        else:
            raise ValueError('Input data must be a dict or dataframe.')

        full_path = f'{gcs_folder}/{filename}'


        try:
            # get the client and bucket
            client = storage.Client(project=project_name)
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(full_path)

            # if the file is a dataframe, store it as a csv
            if isinstance(data, pd.DataFrame):
                # make temp folder and store the csv there
                temp_folder = 'temp'
                os.makedirs(temp_folder, exist_ok=True)
                local_file_path = f'{temp_folder}/{filename}'
                data.to_csv(local_file_path, index=False)

                # upload the csv to gcs
                with open(local_file_path, 'rb') as file:
                    blob.upload_from_file(file)

                # remove the temporary CSV file and folder
                os.remove(local_file_path)
                os.rmdir(temp_folder)

                self.logger.info('Successfully uploaded %s', f'{bucket_name}/{full_path}')

            # if the file is a dict, store it as a json blob
            elif isinstance(data, dict):
                blob.upload_from_string(json.dumps(data),content_type='json')

                self.logger.info('Successfully uploaded %s', f'{bucket_name}/{full_path}')

        except Exception as e:
            self.logger.error(f'Failed to upload {filename} to {bucket_name}/{gcs_folder}: {e}')
            raise
