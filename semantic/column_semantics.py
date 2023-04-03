#!/usr/bin/env python
import requests
from requests.exceptions import HTTPError
import json
import pprint
import csv
import shutil
import os
import sys

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

API_TOKEN = os.getenv('API_TOKEN')
CKAN_URL = os.getenv('CKAN_URL')

# parameters
CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)
OUTPUT_PATH = "./output/datastore_column_info.csv"


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


def get_resources_list() -> (int, list):
    success, result = ckan_api_request("wakeua_list_datastore_resources", "get", API_TOKEN)
    if success >= 0:
        return success, result['result']
    return success, result


def delete_datastore_resource(resource_id: str) -> (int, dict):
    print("\t * DELETE *", resource_id)
    success, result = ckan_api_request("datastore_delete", "post", API_TOKEN,
                                       data={"resource_id": resource_id, 'force': True})
    if success >= 0:
        return success, result['result']
    return success, result


def check_resource(resource_id: str) -> (int, list):
    success, result = ckan_api_request("resource_show", "get", API_TOKEN, params={"id": resource_id})
    if success < 0:
        if result["code"] == 404:
            print("ERROR: NOT FOUND \t" + resource_id)
            print(result["http_error"].errno)
            success, result = delete_datastore_resource(resource_id)
            print(" => DELETED resource: " + resource_id, success, result)
            raise Exception("ERROR: Found deleted resource", resource_id)
        else:
            raise Exception("ERROR: Uknown error check resource", resource_id, success, result)

    return success, result


def get_datastore_info(resource_ids: list) -> list:
    datastore_info = []
    i = 1
    for resource_id in resource_ids:
        print("{}/{}\t".format(i, len(resource_ids)), resource_id)

        success, result = check_resource(resource_id)
        resource_data = result['result']

        print('\t', resource_data['package_id'], resource_data['name']['es'])
        success, result = ckan_api_request("package_show", "get", API_TOKEN, params={"id": resource_data['package_id']})
        package_data = result['result']

        # success, result = ckan_api_request("datastore_info", "post", API_TOKEN, data={"id": id})
        success, result = ckan_api_request("datastore_search", "post", API_TOKEN,
                                           data={"resource_id": resource_id, "limit": 1})
        if success < 0:
            raise Exception("ERROR: Cannot find dataset", resource_id)
        for field in result['result']['fields']:

            field_row = {
                            'id': field.get('id', ''),
                            'type': field.get('type', ''),
                            'label': field.get('info', {}).get('label', ''),
                            'notes': field.get('info', {}).get('notes', ''),
                            'type_override':  field.get('info', {}).get('type_override', ''),
                            'example': '',
                            'resource': resource_data["url"],
                            'dataset_id ': package_data['name'],
                            'dataset ': package_data['url'],
                            'tags': [t["name"] for t in package_data["tags"] if t["name"].endswith("-es")],
                            'vocabulary': package_data.get("tag_string_schemaorg"),
                            'groups': [g["name"] for g in package_data["groups"]],
                            'organization': package_data["organization"]["name"]
            }
            if len(result['result']['records']) > 0:
                field_row['example'] = result['result']['records'][0].get(field.get('id', ''))

            datastore_info += [field_row]
        i += 1
    return datastore_info


def main() -> int:

    # get_all_resources_ids_in_datastore
    success, result = get_resources_list()
    resource_ids = result

    # gather datasets columns
    datastore_info = get_datastore_info(resource_ids)

    with open(OUTPUT_PATH, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=list(datastore_info[0].keys()))
        writer.writeheader()
        writer.writerows(datastore_info)

    print("DONE: Written {} rows to {}".format(len(datastore_info), OUTPUT_PATH))

    return 0


if __name__ == '__main__':
    sys.exit(main())
