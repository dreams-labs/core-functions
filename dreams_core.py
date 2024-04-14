import datetime
import time
import math
import os
import io
import json
import requests
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import storage

# notebook imports (not needed in GCF)
from google.cloud import secretmanager_v1
import pdb


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
