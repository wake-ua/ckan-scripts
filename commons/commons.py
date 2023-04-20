#!/usr/bin/env python
import requests
from requests.exceptions import HTTPError
import os

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

API_TOKEN = os.getenv('API_TOKEN')
CKAN_URL = os.getenv('CKAN_URL')

# CKAN endpoints
CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)


def ckan_api_request(endpoint: str, method: str, data: dict = {},
                     params: dict = {}, files: list = [],
                     content: str = 'application/json', verbose=True) -> (int, dict):
    # set headers
    headers = {'Authorization': API_TOKEN}
    if content:
        headers['Content-Type'] = content

    # do the actual call
    try:
        if method == 'post':
            response = requests.post('{}{}'.format(CKAN_API_URL, endpoint), json=data, params=params,
                                     files=files, headers=headers)
        else:
            response = requests.get('{}{}'.format(CKAN_API_URL, endpoint), params=params, headers=headers)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        result = response.json()
        return 0, result

    except HTTPError as http_err:
        if verbose:
            print(f'\t HTTP error occurred: {http_err} {response.json()}')  # Python 3.6
        result = {"http_error": http_err, "error": response.json()}
    except Exception as err:
        if verbose:
            print(f'\t Other error occurred: {err}')  # Python 3.6
        result = {"error": err}

    return -1, result