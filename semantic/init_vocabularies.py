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
FILE_PATH = os.getenv('TAG_LIST_PATH')

# parameters
CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)
VOCABULARY_NAME = "schemaorg"
LANGS = ["es", "ca", "en"]

def read_tags(file_path: str) -> list:
    # read the tags file
    print(" - Read input file: {}".format(file_path))

    tags = []

    with open(FILE_PATH) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for row in reader:
            new_row = { }

            for field, value in row.items():
                if field.rsplit('_', 1)[-1] in LANGS:
                    parent_field = field.rsplit('_', 1)[0]
                    lang = field.rsplit('_', 1)[-1]
                    translated_field = new_row.get(parent_field, {})
                    translated_field[lang] = value
                    new_row[parent_field] = translated_field
                else:
                    new_row[field] = value

            tags += [new_row]
            print("\t * {}".format(new_row))

    print(" \t => Read {} tags(s): {}".format(len(tags), ', '.join([tag['tag_vocabulary']["es"] for tag in tags])))

    return tags


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


def edit_vocabulary(name: str, tags: list, update: bool = False) -> (int, dict):
    # map attributes to ckan tag
    ckan_vocabulary = {
        "name": name,
        "tags": [{"name": tag["tag_vocabulary"][lang].strip().replace("'", " ").replace("’", " ") + "-" + lang} for tag in tags for lang in LANGS]
    }

    # call the endpoint
    if not update:
        success, result = ckan_api_request(endpoint="vocabulary_create", method="post", token=API_TOKEN,
                                           data=ckan_vocabulary)
    else:
        ckan_vocabulary["id"] = name
        success, result = ckan_api_request(endpoint="vocabulary_update", method="post",
                                           token=API_TOKEN, data=ckan_vocabulary)
    return success, result


def add_datasets(tags: list) -> (int, dict):
    datasets = {}
    for tag in tags:
        tag_datasets = [dataset.strip() for dataset in tag["datasets"].split(" ") if dataset]
        for dataset in tag_datasets:
            datasets[dataset] = datasets.get(dataset, []) + [tag["name"]]
    for dataset, tag_ids in datasets.items():
        print("\t\t - Adding tags to dataset {}: {}".format(dataset, ", ".join(tag_ids)))
        ckan_dataset = {"id": dataset, "tags": [{"name": tag} for tag in tag_ids]}
        print(ckan_dataset)
        success, result = ckan_api_request(endpoint="package_patch", method="post",
                                           token=API_TOKEN, data=ckan_dataset)
        if success < 0:
            raise Exception("Could not patch dataset " + str(result))
        print(result)
    return success, result


def main() -> int:

    # read the input file
    tags = read_tags(FILE_PATH)

    # create the vocabulary
    print("\n * Creating vocabulary: {}".format(VOCABULARY_NAME))
    success, result = edit_vocabulary(VOCABULARY_NAME, tags)
    if success >= 0:
        print("\t * Created: {}".format(result))
    else:
        print("\t => Created Failed, trying UPDATE...")
        success, result = edit_vocabulary(VOCABULARY_NAME, tags, update=True)
        if success >= 0:
            print("\t * Updated: {}".format(result))
        else:
            print("\t => * Update Failed *")
            return -1

    print(" * Finished: \n\t - Created/Updated Vocabulary {} with {} tags: {} "
          .format(VOCABULARY_NAME, len(result['result']['tags']),
                  ', '.join([tag["name"] for tag in result['result']['tags']])))

    return 0


if __name__ == '__main__':
    sys.exit(main())
