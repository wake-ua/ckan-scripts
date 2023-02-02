#!/usr/bin/env python
import requests
from requests.exceptions import HTTPError
import json
import pprint
import csv
import io
import shutil
import os
import sys

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

API_TOKEN = os.getenv('API_TOKEN')
CKAN_URL = os.getenv('CKAN_URL')

# parameters
FILE_DIR = "./data"
PREVIEW_LINES = 10

CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)

default_locations = {"ajuntament-alcoi": {"coordinates": [-0.47464877036321695, 38.69780773097776],
                                          "type": "Point"
                                          }
                     }


def read_datasets(file_dir: str) -> list:

    # read the datasets files
    print(" - Read input dir: {}".format(file_dir))

    datasets = []

    for file in os.listdir(file_dir):
        if file.endswith(".json"):
            file_path = os.path.join(file_dir, file)
            print("\t * Read ", file_path)

            with open(file_path) as jsonfile:

                dataset = json.load(jsonfile)["result"]
                datasets += [dataset]

                print("\t\t => Got '{}' ({})".format(dataset["name"], dataset["organization"]["name"]))

    print(" \t => Read {} dataset(s): {}".format(len(datasets),
                                                 ', '.join([dataset['name'] for dataset in datasets])))

    return datasets


def ckan_api_request(endpoint: str, method: str, token: str, data: dict = {}, params: dict = {}) -> (int, dict):
    # set headers
    headers = {'Authorization': token,
               'Content-Type': 'application/json'}

    result = {}

    # do the actual call
    try:
        if method == 'post':
            response = requests.post('{}{}'.format(CKAN_API_URL, endpoint), data=json.dumps(data), params=params, headers=headers)
        else:
            response = requests.get('{}{}'.format(CKAN_API_URL, endpoint), params=params, headers=headers)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        result = response.json()
        return 0, result

    except HTTPError as http_err:
        print(f'\t HTTP error occurred: {http_err} {response.json()}')  # Python 3.6
        result = {"http_error": http_err, "error": response.json()}
    except Exception as err:
        print(f'\t Other error occurred: {err}')  # Python 3.6
        result = {"error": err}

    return -1, result


def edit_dataset(dataset: dict, update: bool = False) -> (int, dict):

    # map attributes to ckan dataset
    ckan_dataset = {
        "name": dataset["name"],
        "title": dataset["title"]["es"],
        "notes": dataset["notes"]["es"],
        "owner_org": dataset["organization"]["name"],
        "license_id": dataset["license_id"],
        "spatial": json.dumps(default_locations[dataset["organization"]["name"]])
        # "extras": [{"key": "spatial", "value": json.dumps(default_locations[dataset["organization"]["name"]])}]
    }

    # check resources
    ckan_resources = []
    for resource in dataset["resources"]:
        ckan_resource = {}
        ckan_resource["url"] = resource["url"]
        ckan_resource["name"] = resource["name_es"]
        ckan_resource["description"] = resource.get("description_es", resource["description"])
        ckan_resource["format"] = resource["format"]
        ckan_resource["size"] = resource["size"]
        ckan_resource["mimetype"] = resource["mimetype"]

        if resource["url"].endswith(".csv"):
            print("\t * CSV Resource found: {}".format(resource["url"].split("/")[-1]))
            response = requests.get(resource["url"])
            buff = io.StringIO(response.text)
            cr = csv.DictReader(buff)
            selected_lines = []
            for row in cr:
                selected_lines += [row]
                if len(selected_lines) >= PREVIEW_LINES:
                    break

            if selected_lines:
                header = [k for k in selected_lines[0].keys()]
                ckan_resource["description"] += "\n Data header: " + ', '.join(header)

        ckan_resources += [ckan_resource]

    if ckan_resources:
        ckan_dataset["resources"] = ckan_resources

    # call the endpoint
    if not update:
        success, result = ckan_api_request(endpoint="package_create", method="post", token=API_TOKEN,
                                           data=ckan_dataset)
    else:
        ckan_dataset["id"] = dataset["name"]
        success, result = ckan_api_request(endpoint="package_patch", method="post",
                                           token=API_TOKEN, data=ckan_dataset)
    return success, result


def main() -> int:
    created_datasets = []
    updated_datasets = []

    # read the input file
    datasets = read_datasets(FILE_DIR)

    # save the organizations
    for dataset in datasets:
        print("\n * Creating DATA: {}".format(dataset["name"]))
        success, result = edit_dataset(dataset)
        if success >= 0:
            print("\t * Created: {}".format(result))
            created_datasets += [dataset["name"]]
        else:
            print("\t => Created Failed, trying UPDATE...")
            success, result = edit_dataset(dataset, update=True)
            if success >= 0:
                print("\t * Updated: {}".format(result))
                updated_datasets += [dataset["name"]]
            else:
                print("\t => * Update Failed *")
                return -1

    print(" * Finished: \n\t - Created {} datasets: {} "
          "\n\t - Updated {} datasets: {}".format(len(created_datasets), ', '.join(created_datasets),
                                                  len(updated_datasets), ', '.join(updated_datasets)))

    success, total_datasets = ckan_api_request(endpoint="package_list", method="get", token=API_TOKEN)
    if success >= 0:
        print("\nCKAN Datasets ({}): {}".format(len(total_datasets["result"]), ', '.join(total_datasets["result"])))
    else:
        print("\t => * Retrieving All Datasets Failed *")
        return -1

    return 0


if __name__ == '__main__':
    sys.exit(main())
