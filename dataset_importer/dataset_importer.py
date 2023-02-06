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
import datetime

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

API_TOKEN = os.getenv('API_TOKEN')
CKAN_URL = os.getenv('CKAN_URL')

# parameters
FILE_DIR = "./data"
TMP_DIR = os.path.join(FILE_DIR, 'tmp')
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


def ckan_api_request(endpoint: str, method: str, token: str, data: dict = {},
                     params: dict = {}, files: list = [], dump: bool = True,
                     content: str = 'application/json') -> (int, dict):
    # set headers
    headers = {'Authorization': token}
    if content:
        headers['Content-Type'] = content

    # do the actual call
    try:
        if method == 'post':
            if dump:
                data = json.dumps(data)
            response = requests.post('{}{}'.format(CKAN_API_URL, endpoint), data=data, params=params,
                                     files=files, headers=headers)
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


def handle_csv_resource(ckan_resource: dict) -> (str, dict):

    file_name = ckan_resource["url"].split("/")[-1]
    response = requests.get(ckan_resource["url"])
    buff = io.StringIO(response.text)
    cr = csv.DictReader(buff)
    selected_lines = []
    for row in cr:
        selected_lines += [row]
        if len(selected_lines) >= PREVIEW_LINES:
            break

    if selected_lines:
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
        file_name = os.path.join(TMP_DIR, 'PREVIEW_' + timestamp + '_' + ckan_resource["url"].split("/")[-1])
        header = [k for k in selected_lines[0].keys()]

        print("\t  *  Saving preview at: " + file_name)
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header)

            writer.writeheader()
            for line in selected_lines:
                writer.writerow(line)

        ckan_resource["description"] += "\n\n Data header: " + ', '.join(header)
        ckan_resource["description"] += "\n\n Full data available at: " + ckan_resource["url"]

    return file_name, ckan_resource


def edit_dataset(dataset: dict, update: bool = False) -> (int, dict):

    # map attributes to ckan dataset
    ckan_dataset = {
        "name": dataset["name"],
        "title": dataset["title"]["es"],
        "notes": dataset["notes"]["es"],
        "owner_org": dataset["organization"]["name"],
        "license_id": dataset["license_id"],
        "spatial": json.dumps(default_locations[dataset["organization"]["name"]])
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


def update_resource(resource: dict) -> (int, dict):

    success = -1
    result = {}

    if resource["url"].endswith(".csv"):
        file_name, ckan_resource = handle_csv_resource(resource)
        if os.path.isfile(file_name):
            # call the update resource endpoint
            keys = ["description", "format", "id", "mimetype", "name", "package_id", "size"]
            data = {k: ckan_resource[k] for k in keys}
            success, result = ckan_api_request(endpoint="resource_create", method="post",
                                               token=API_TOKEN, data=data,
                                               files=[('upload', open(file_name, 'rb'))], dump=False,
                                               content="")
                                               # content="multipart/form-data")
            os.remove(file_name)

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
            print("\t * Created: {}...".format(str(result)[:500]))
            created_datasets += [dataset["name"]]
        else:
            print("\t => Created Failed, trying UPDATE...")
            success, result = edit_dataset(dataset, update=True)
            if success >= 0:
                print("\t * Updated: {}...".format(str(result)[:500]))
                updated_datasets += [dataset["name"]]
            else:
                print("\t => * Update Failed *")
                return -1

        updated_dataset = result["result"]

        # add previews to csv resources
        for resource in updated_dataset["resources"]:
            if resource["url"].endswith(".csv"):
                print("\t * UPDATING csv resource {} (preview upload)...".format(resource['name']))
                success, result = update_resource(resource)
                if success >= 0:
                    print("\t * Updated csv resource : {}...".format(str(result)[:500]))
                else:
                    print("\t => ERROR: * Resource csv pdate Failed *")
                    return -1

    print(" \t - Created {} datasets: {} "
          "\n\t - Updated {} datasets: {}".format(len(created_datasets), ', '.join(created_datasets),
                                                  len(updated_datasets), ', '.join(updated_datasets)))

    success, total_datasets = ckan_api_request(endpoint="package_list", method="get", token=API_TOKEN)
    if success >= 0:
        print("\n - CKAN Datasets ({}): {}".format(len(total_datasets["result"]), ', '.join(total_datasets["result"])))

    else:
        print("\t => * ERROR: Retrieving All Datasets Failed *")
        return -1

    return 0


if __name__ == '__main__':
    sys.exit(main())
