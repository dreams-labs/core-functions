import datetime
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage

class BigQuery:
    ''' 
    A class to interact with BigQuery. This class is designed
    to be used in the context of the Dreams project. It is not
    intended to be a general purpose BigQuery class.
    '''

    def __init__(
            self,
            service_account_json,
            verbose=False
        ):
        self.service_account_json = service_account_json
        self.verbose = verbose
        self.location = 'US'
        self.project_id = 'western-verve-411004'
        self.project_name = 'dreams-labs-data'
        self.bucket_name = 'dreams-labs-storage'


    def run_sql(
            self,
            query_sql
        ):
        '''
        runs a query and returns results as a dataframe. the service account credentials to 
        grant access are autofilled to a general service account. there are likely security
        optimizations that will need to be done in the future. 

        param: query_sql <string> the query to run
        param: service_account_json_secret <json> the name of the GCP secrets manager secret that \
            contains the service account jeson data
        param: location <string> the location of the bigquery project
        param: project <string> the project ID of the bigquery project
        return: query_df <dataframe> the query result
        '''
        # prepare credentials using a service account stored in GCP secrets manager

        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        credentials = service_account.Credentials.from_service_account_info(
            self.service_account_json
            ,scopes=scopes
        )

        # create a bigquery client object and run the query
        client = bigquery.Client(project=self.project_id, location=self.location, credentials=credentials)
        query_job = client.query(query_sql)
        query_df = query_job.to_dataframe()

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

        credentials = service_account.Credentials.from_service_account_info(self.service_account_json)

        filepath = f'cache/query_{cache_file_name}.csv'
        client = storage.Client(project=self.project_name, credentials=credentials)
        bucket = client.get_bucket(self.bucket_name)

        # Attempt to retrieve file freshness
        try:
            blob = bucket.get_blob(filepath)
            file_freshness = blob.updated if blob else None
        except Exception as e:
            print(f"error retrieving blob: {e}")
            file_freshness = None

        # Determine cache staleness
        cache_stale = file_freshness is None or \
                    (datetime.datetime.now(tz=datetime.timezone.utc) - file_freshness).total_seconds() / 3600 > freshness

        # Refresh cache if stale
        if cache_stale:
            query_df = self.run_sql(query_sql)
            blob = bucket.blob(filepath)
            blob.upload_from_string(query_df.to_csv(index=False), content_type='text/csv')
            if self.verbose:
                print('returned fresh csv and refreshed cache')
        else:
            query_df = pd.read_csv(f'gs://{self.bucket_name}/{filepath}')
            if self.verbose:
                print('returned cached csv')

        return query_df
