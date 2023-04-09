#!/usr/bin/env python
import requests
from requests.exceptions import HTTPError
import csv
import os
import sys

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

API_TOKEN = os.getenv('API_TOKEN')
CKAN_URL = os.getenv('CKAN_URL')
FILE_PATH = os.getenv('DATASET_LIST_PATH')

# parameters
CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)
CKAN_UPLOADS_URL = "{}/uploads/group/".format(CKAN_URL)


def read_datasets(file_path: str) -> (dict, dict):
    # read the groups file
    print(" - Read input file: {}".format(file_path))

    datasets_to_delete = {}
    datasets_to_add = {}

    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for row in reader:
            if row['ok'] == '0':
                datasets_to_delete[row['id']] = row
            else:
                datasets_to_add[row['id']] = row

    print(" \t => Read {} dataset(s) to delete: {}...".format(len(datasets_to_delete.keys()),
                                                              ', '.join(list(datasets_to_delete.keys())[0:10])))
    print(" \t => Read {} dataset(s) to add: {}...".format(len(datasets_to_add.keys()),
                                                           ', '.join(list(datasets_to_add.keys())[0:10])))

    return datasets_to_delete, datasets_to_add

def delete_datasets(datasets: dict) -> int:
    count = 0

    for dataset_id, dataset in datasets.items():
        try:
            ckan_api_request('package_delete', 'post', API_TOKEN, {'id': dataset_id})
            count += 1
        except:
            pass

    return count


def ckan_api_request(endpoint: str, method: str, token: str, data: dict = {}, params: dict = {}) -> (int, dict):
    # set headers
    headers = {'Authorization': token}

    result = {}

    # do the actual call
    try:
        if method.lower() == 'post':
            response = requests.post('{}{}'.format(CKAN_API_URL, endpoint), json=data, params=params, headers=headers)
        else:
            response = requests.get('{}{}'.format(CKAN_API_URL, endpoint), params=params, headers=headers)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        result = response.json()
        return 0, result

    except HTTPError as http_err:
        print(f'\t HTTP error occurred: {http_err} {response.json().get("error")}')  # Python 3.6
        result = {"http_error": http_err, "error": response.json().get("error")}
    except Exception as err:
        print(f'\t Other error occurred: {err}')  # Python 3.6
        result = {"error": err}

    return -1, result


def main() -> int:

    # read the input file
    datasets_to_delete, datasets_to_add = read_datasets(FILE_PATH)

    deleted = delete_datasets(datasets_to_delete)
    print("* DELETED {} datasets".format(deleted))

    return 0


if __name__ == '__main__':
    sys.exit(main())
