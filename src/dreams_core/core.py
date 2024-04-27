'''
this module includes core functions for the dreams labs data ecosystem. functions included here 
are designed to be broadly applicable and resuable across many projects. functions speciifc to 
individual tools such as dune/bigquery/etc are available in other modules within this directory. 
'''

import numpy as np
import google.auth
from google.cloud import secretmanager_v1
from google.oauth2 import service_account
from .googlecloud import GoogleCloud as dgc


def human_format(number):
    '''
    converts a number to a scaled human readable string (e.g 7437283-->7.4M)

    logic:
        1. handle 0s
        2. for 0.XX inputs, include 2 significant figures (e.g. 0.00037, 0.40, 0.0000000011)
        3. for larger numbers, reducing to 1 significant figure and add 'k', 'M', 'B', etc

    TODO: the num<1 code should technically round upwards when truncating the
    string, e.g. 0.0678 right now will display as 0.067 but should be 0.068

    param: num <numeric>: the number to be reformatted
    return: formatted_number <string>: the number formatted as a human-readable string
    '''
    suffixes = ['', 'k', 'M', 'B', 'T', 'Qa', 'Qi', 'Sx', 'Sp', 'O', 'N', 'D']

    # 1. handle 0s
    if number == 0:
        return '0'

    # 2. handle decimal type inputs
    if -1 < number < 1:
        # decimals are output with enough precision to show two significant figures

        # whether number is returned negative
        if number < 0:
            negative_prefix='-'
        else:
            negative_prefix=''

        # determine how much of initial string to keep
        number = np.format_float_positional(abs(number))
        after_decimal = str(number[2:])
        keep = 4+len(after_decimal) - len(after_decimal.lstrip('0'))

        return f'{negative_prefix}{str(number[:keep])}'

    # 3. handle non-decimal type inputs
    i = 0
    while abs(number) >= 1000:
        number /= 1000.0
        i += 1

    return f'{number:.1f}{suffixes[i]}'


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
    if service_account_path:
        # Explicitly use the provided service account file for credentials
        credentials = service_account.Credentials.from_service_account_file(service_account_path)
    else:
        # Attempt to use default credentials
        credentials, _ = google.auth.default()
    client = secretmanager_v1.SecretManagerServiceClient(credentials=credentials)

    # Request to access the secret version
    request = secretmanager_v1.AccessSecretVersionRequest(name=secret_path)
    response = client.access_secret_version(request=request)
    return response.payload.data.decode('UTF-8')


def translate_chain(
        input_chain
        ,verbose=False
    ):
    '''
    Attempts to match a blockchain alias and returns a dictionary with all
    corresponding aliases.

    Args:
        input_chain (str): The chain name input by the user.
        verbose (bool): Whether to print debugging information.

    Returns:
        dict: A dictionary with all available chain aliases.
    '''

    # retreive chain ids for all aliases
    query_sql = '''
        select cn.chain_id
        ,cn.chain_reference
        ,ch.*
        from reference.chain_nicknames cn
        left join core.chains ch on ch.chain_id = cn.chain_id
        '''
    chain_nicknames_df = dgc().cache_sql(query_sql,'chain_nicknames')

    # set everything to be lower case
    chain_nicknames_df['chain_reference'] = chain_nicknames_df['chain_reference'].str.lower()
    input_chain = input_chain.lower()


    # filter the df of all aliases for the input chain
    input_chain_nicknames_df = chain_nicknames_df[chain_nicknames_df['chain_reference'] == input_chain]

    # if the input chain alias couldn't be found, return empty dict
    if input_chain_nicknames_df.empty:
        if verbose:
            print(f'input value "{input_chain}" could not be matched to any known chain alias')
        return {}

    # if the input chain alias could be found, store its id and name in a dictionary
    chain_dict = {
        'chain_id': input_chain_nicknames_df['chain_id'].iloc[0],
        'chain_name': input_chain_nicknames_df['chain'].iloc[0]
    }

    # add all additional chain aliases to the dictionary
    chain_text_columns = chain_nicknames_df.filter(regex='chain_text_').columns
    for column in chain_text_columns:
        nickname = input_chain_nicknames_df[column].iloc[0]
        if nickname:
            chain_dict[column.replace('chain_text_', '')] = nickname

    if verbose:
        print(f'retrieved chain nicknames for {str(chain_dict.keys())}')

    return chain_dict
