#!/usr/bin/env python
import requests
from requests.exceptions import HTTPError
import os
import sys
import csv

import subprocess
import time

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

API_TOKEN = os.getenv('API_TOKEN')
CKAN_URL = os.getenv('CKAN_URL')
CKAN_CONFIG = os.getenv('CKAN_CONFIG')

# parameters
CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)
OUTPUT_PATH = "./output/dataset_list.csv"


def ckan_api_request(endpoint: str, method: str, token: str, data: dict = {}, params: dict = {}) -> (int, dict):
    # set headers
    headers = {'Authorization': token}

    result = {}

    # do the actual call
    try:
        if method == 'post':
            response = requests.post('{}{}'.format(CKAN_API_URL, endpoint), json=data, params=params, headers=headers)
        else:
            response = requests.get('{}{}'.format(CKAN_API_URL, endpoint), params=params, headers=headers)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        result = response.json()
        return 0, result

    except HTTPError as http_err:
        print(f'\t HTTP error occurred: {http_err} {response.json().get("error")}')  # Python 3.6
        result = {"code": response.status_code, "http_error": http_err, "error": response.json().get("error")}
    except Exception as err:
        print(f'\t Other error occurred: {err}')  # Python 3.6
        result = {"error": err}

    return -1, result


def save_datasets_list() -> int:
    count = 0
    writer = None
    step = 500

    success, result = ckan_api_request("organization_list", "get", API_TOKEN)
    organizations = result['result']

    with open(OUTPUT_PATH, 'w') as f:
        for organization in organizations:
            print("\n * Organization", organization)
            params = {"q": "organization:{}".format(organization), "rows": step}
            success, result = ckan_api_request("package_search", "get", API_TOKEN, params=params)
            if success < 0:
                raise("ERROR: Cannot retrieve datasets", organization)
            total = result['result']['count']
            datasets = result['result']['results']

            while len(datasets) < total and len(result['result']['results']) > 0:
                params = {"q": "organization:{}".format(organization), "rows": step, "start": len(datasets)}
                success, result = ckan_api_request("package_search", "get", API_TOKEN, params=params)
                if success < 0:
                    raise ("ERROR: Cannot retrieve datasets")
                datasets += result['result']['results']

            print("\t => Total", len(datasets))

            count += len(datasets)

            for dataset in datasets:
                row = {
                    'ok': '',
                    'organization': dataset["organization"]["name"],
                    'id': dataset.get('name', ''),
                    'title': dataset.get('title', {}).get('es', ''),
                    'url': dataset.get('url'),
                    'tags': '"'
                            + ','.join([t["name"][:-3] for t in dataset["tags"] if t["name"].endswith("-es")])
                            + '"',
                    'vocabulary': '"'
                            + ','.join([t[:-3] for t in dataset.get("tag_string_schemaorg", "").split(',') if t.endswith("-es")])
                            + '"',
                    'groups': '"'
                            + ','.join([g["name"] for g in dataset["groups"]])
                            + '"',
                    'original_tags': dataset.get("original_tags",'')
                }
                if len(row['groups'])>2:
                    row['ok'] = 1
                print(row.values())

                if not writer:
                    writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                    writer.writeheader()
                writer.writerows([row])

    return count


def main() -> int:

    # save in the csv all te datasets
    count = save_datasets_list()
    print("Total:", count)

    return 0


if __name__ == '__main__':
    sys.exit(main())
