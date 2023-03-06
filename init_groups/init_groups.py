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

UPLOADS_PATH = os.getenv('GROUP_IMAGE_UPLOADS_PATH')
API_TOKEN = os.getenv('API_TOKEN')
CKAN_URL = os.getenv('CKAN_URL')
FILE_PATH = os.getenv('GROUP_LIST_PATH')

# parameters
IMAGE_DIR = "./data/image"

CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)
CKAN_UPLOADS_URL = "{}/uploads/group/".format(CKAN_URL)


def read_groups(file_path: str) -> list:
    # read the groups file
    print(" - Read input file: {}".format(file_path))

    groups = []

    with open(FILE_PATH) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for row in reader:
            groups += [row]
            print("\t * {}".format(row))

    print(" \t => Read {} groups(s): {}".format(len(groups),
                                                      ', '.join([group['name'] for group in groups])))

    return groups


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
        result = {"http_error": http_err, "error": response.json().get("error")}
    except Exception as err:
        print(f'\t Other error occurred: {err}')  # Python 3.6
        result = {"error": err}

    return -1, result


def edit_group(group: dict, update: bool = False) -> (int, dict):
    # map attributes to ckan group
    ckan_group = {
        "name": group["name"],
        "title": {lang: group["title" + "_" + lang] for lang in ["es", "ca", "en"]},
        "description": {lang: group["description" + "_" + lang] for lang in ["es", "ca", "en"]},
        "image_display_url": "{}/{}_logo.png".format(CKAN_UPLOADS_URL, group["name"]),
        "image_url": "{}_logo.png".format(group["name"])
    }

    # copy image to the uploads
    shutil.copyfile("{}/{}_logo.png".format(IMAGE_DIR, group["name"]),
                    "{}/{}_logo.png".format(UPLOADS_PATH, group["name"]))

    # call the endpoint
    if not update:
        success, result = ckan_api_request(endpoint="group_create", method="post", token=API_TOKEN,
                                           data=ckan_group)
    else:
        ckan_group["id"] = group["name"]
        success, result = ckan_api_request(endpoint="group_patch", method="post",
                                           token=API_TOKEN, data=ckan_group)
    return success, result


def add_datasets(groups: list) -> (int, dict):
    datasets = {}
    for group in groups:
        group_datasets = [dataset.strip() for dataset in group["datasets"].split(" ") if dataset]
        for dataset in group_datasets:
            datasets[dataset] = datasets.get(dataset, []) + [group["name"]]
    for dataset, group_ids in datasets.items():
        print("\t\t - Adding groups to dataset {}: {}".format(dataset, ", ".join(group_ids)))
        ckan_dataset = {"id": dataset, "groups": [{"name": group} for group in group_ids]}
        print(ckan_dataset)
        success, result = ckan_api_request(endpoint="package_patch", method="post",
                                           token=API_TOKEN, data=ckan_dataset)
        if success < 0:
            raise Exception("Could not patch dataset " + str(result))
        print(result)
    return success, result


def main() -> int:
    created_groups = []
    updated_groups = []

    # read the input file
    groups = read_groups(FILE_PATH)

    # save the groups
    for group in groups:
        print("\n * Creating group: {}".format(group["name"]))
        success, result = edit_group(group)
        if success >= 0:
            print("\t * Created: {}".format(result))
            created_groups += [group["name"]]
        else:
            print("\t => Created Failed, trying UPDATE...")
            success, result = edit_group(group, update=True)
            if success >= 0:
                print("\t * Updated: {}".format(result))
                updated_groups += [group["name"]]
            else:
                print("\t => * Update Failed *")
                return -1
    # add datasets
    print("\t * Adding datasets to groups: ")
    success, result = add_datasets(groups)
    if success < 0:
        print("\t => * Adding datasets Failed *")
        return -1

    print(" * Finished: \n\t - Created {} groups: {} "
          "\n\t - Updated {} groups: {}".format(len(created_groups), ', '.join(created_groups),
                                                     len(updated_groups), ', '.join(updated_groups)))

    success, total_groups = ckan_api_request(endpoint="group_list", method="get", token=API_TOKEN)
    if success >= 0:
        print("\nCKAN groups: {}".format(', '.join(total_groups["result"])))
    else:
        print("\t => * Retrieving All groups Failed *")
        return -1

    return 0


if __name__ == '__main__':
    sys.exit(main())
