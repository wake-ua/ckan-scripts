#!/usr/bin/env python
import requests
from requests.exceptions import HTTPError
import json
import pprint
# import csv
import io
import shutil
import os
import sys
import datetime
# from django.contrib.gis.geos import Polygon

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

API_TOKEN = os.getenv('API_TOKEN')
CKAN_URL = os.getenv('CKAN_URL')
ORGANIZATIONS_FILE_PATH = os.getenv('ORGANIZATION_LIST_PATH')

# parameters
CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)
LANGS = ['es', 'ca', 'en']


def get_datasets_list(file_dir: str) -> list:
    # read the datasets files
    print(" - Read input dir: {}".format(file_dir))

    datasets = []

    for file in os.listdir(file_dir):
        if file.endswith(".json"):
            file_path = os.path.join(file_dir, file)
            datasets += [file_path]

    return datasets


def read_organization(organization_name: str) -> dict:
    # read the dataset file and transform to json dict
    with open(ORGANIZATIONS_FILE_PATH) as jsonfile:
        organizations = json.load(jsonfile)["organizations"]
        for organization in organizations:
            if organization["name"] == organization_name:
                return organization
    return {}


def read_dataset(file_path: str) -> dict:
    # read the dataset file and transform to json dict
    with open(file_path) as jsonfile:
        dataset = json.load(jsonfile)
        dataset = dataset.get("result", dataset)
    return dataset


def ckan_api_request(endpoint: str, method: str, token: str, data: dict = {},
                     params: dict = {}, files: list = [],
                     content: str = 'application/json') -> (int, dict):
    # set headers
    headers = {'Authorization': token}
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
        print(f'\t HTTP error occurred: {http_err} {response.json()}')  # Python 3.6
        result = {"http_error": http_err, "error": response.json()}
    except Exception as err:
        print(f'\t Other error occurred: {err}')  # Python 3.6
        result = {"error": err}

    return -1, result


def get_translated_field(field: str, dataset: dict, default_value: str, org_name: str, mandatory: bool = False) -> dict:
    new_field = {'es': "", 'ca': "", "en": ""}

    for lang in new_field.keys():
        value = ""
        data = dataset.get(field, {})
        if isinstance(data, dict):
            value = data.get(lang, "")
        if not value:
            value = dataset.get(field + '_' + lang, "")
        if not value:
            if lang == "es":
                data = dataset.get(field, "")
                if isinstance(data, str):
                    value = data
                if not value:
                    value = default_value
            else:
                value = new_field["es"]
        if value:
            new_field[lang] = value

    # special cases
    if org_name == "valencia":
        multi_value = new_field["es"].split(" / ")
        if len(multi_value) == 2:
            new_field["ca"] = multi_value[0]
            new_field["es"] = multi_value[1]
            new_field["en"] = multi_value[1]

    if mandatory:
        langs = list(new_field.keys())
        some_value = dataset["identifier"]
        for lang in langs:
            if len(new_field[lang]) > 0:
                some_value = new_field[lang]
                break
        for lang in langs:
            if len(new_field[lang]) == 0:
                new_field[lang] = some_value

    return new_field


def edit_dataset(dataset: dict, organization: dict, update: bool = False, resource_ids: dict ={}) -> (int, dict):
    # map attributes to ckan dataset
    ckan_dataset = {
        "name": organization["name"] + "-" + dataset["identifier"],
        "title": get_translated_field("title", dataset, dataset["identifier"], organization["name"], True),
        "notes": get_translated_field("description", dataset, dataset["title"], organization["name"]),
        "url": organization["source"] + "/explore/dataset/" + dataset["identifier"],
        "owner_org": organization["name"],
        "license_id": dataset["license"],
        "spatial": json.dumps(organization["spatial"])}

    # check resources
    ckan_resources = []
    for resource in dataset["resources"]:
        ckan_resource = {}
        # ckan_resource["id"] = resource["id"]

        # handle urls
        ckan_resource["url"] = resource["downloadUrl"][0]
        other_urls = []
        i = 0
        for url in resource["downloadUrl"]:
            if resource["mediaType"][i].lower().strip() == 'csv':
                ckan_resource["url"] = url
            else:
                other_urls += [(resource["mediaType"][i].lower().strip(), url)]
            i += 1

        ckan_resource["name"] = {lang: resource["path"].split('/')[-1].split('.')[0] for lang in LANGS}

        if update:
            resource_ids = get_resource_ids(ckan_dataset)
            if resource_ids.get(ckan_resource["name"]["es"], False):
                ckan_resource["id"] =resource_ids[ckan_resource["name"]["es"]]

        ckan_resource["description"] = {lang: "" for lang in LANGS}
        ckan_resource["format"] = ckan_resource["url"].split('/')[-1]
        # ckan_resource["size"] = resource["size"]
        ckan_resource["mimetype"] = resource["path"].split('.')[-1]
        ckan_resources += [ckan_resource]

    if ckan_resources:
        ckan_dataset["resources"] = ckan_resources

    # call the endpoint
    if not update:
        success, result = ckan_api_request(endpoint="package_create", method="post", token=API_TOKEN,
                                           data=ckan_dataset)
    else:
        ckan_dataset["id"] = ckan_dataset["name"]
        success, result = ckan_api_request(endpoint="package_patch", method="post",
                                           token=API_TOKEN, data=ckan_dataset)
    return success, result


def get_resource_ids(dataset: dict) -> dict:
    resource_ids = {}
    success, result = ckan_api_request(endpoint="package_show", method="get",
                                           token=API_TOKEN, params={"id": dataset["name"]})
    ckan_dataset = result["result"]
    for resource in ckan_dataset.get("resources", []):
        resource_ids[resource["name"]["es"]] = resource["id"]
    return resource_ids


def main() -> int:

    # get commmandline params

    input_dir = "./data/valenciaOpenDataSoft"
    organization_name = "valencia"

    # input_dir = "./data/datosAbiertosDipCas"
    # organization_name = "dipcas"

    selected_package = None
    if len(sys.argv) > 2:
        input_dir = sys.argv[1]
        organization_name = sys.argv[2]
        if len(sys.argv) > 3:
            selected_package = sys.argv[3]

    print(" * Reading {} datasets from {}".format(input_dir, organization_name))

    # get the organization data
    organization = read_organization(organization_name)
    print(" * Got organization {} ({}) data".format(organization["title"]["en"], organization['name']))

    # process the datasets
    created_datasets = []
    updated_datasets = []

    # read the input file
    dataset_files = get_datasets_list(input_dir)
    print("\t -Got {} datasets".format(len(dataset_files)))

    # save the organizations
    for dataset_file in dataset_files:
        print("* Reading dataset {}".format(dataset_file))
        dataset = read_dataset(dataset_file)

        if selected_package and dataset["identifier"] != selected_package:
            print(" \t\t Skipping dataset {} (!= {})".format(dataset["identifier"], selected_package))
            continue

        print("\n * Creating DATA: {}".format(dataset["identifier"]))
        success, result = edit_dataset(dataset, organization)
        if success >= 0:
            print("\t * Created: {}...".format(str(result)[:500]))
            created_datasets += [dataset["identifier"]]
        else:
            print("\t => Created Failed, trying UPDATE...")
            success, result = edit_dataset(dataset, organization, update=True)
            if success >= 0:
                print("\t * Updated: {}...".format(str(result)[:500]))
                updated_datasets += [dataset["identifier"]]
            else:
                print("\t => * Update Failed *")
                return -1

    print(" \t - Created {} datasets: {} "
          "\n\t - Updated {} datasets: {}".format(len(created_datasets), ', '.join(created_datasets),
                                                  len(updated_datasets), ', '.join(updated_datasets)))

    # success, total_datasets = ckan_api_request(endpoint="package_list", method="get", token=API_TOKEN)
    # if success >= 0:
    #     print("\n - CKAN Datasets ({}): {}".format(len(total_datasets["result"]), ', '.join(total_datasets["result"])))
    #
    # else:
    #     print("\t => * ERROR: Retrieving All Datasets Failed *")
    #     return -1

    return 0


if __name__ == '__main__':
    sys.exit(main())
