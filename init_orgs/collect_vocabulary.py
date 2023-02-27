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
FILE_PATH = os.getenv('ORGANIZATION_LIST_PATH')

# parameters
OUTPUT_PATH = "./data/semantic"



def read_organizations(file_path: str) -> list:
    # read the organizations file
    print(" - Read input file: {}".format(file_path))

    organizations = []

    with open(FILE_PATH) as jsonfile:
        organizations = json.load(jsonfile)["organizations"]

    print(" \t => Read {} organization(s): {}".format(len(organizations),
          ', '.join([org['name'] for org in organizations])))

    return organizations


def ckan_api_request(url: str, endpoint: str, method: str, token: str, data: dict = {}, params: dict = {}) -> (int, dict):
    # set headers
    headers = {'Authorization': token}

    result = {}

    # do the actual call
    try:
        if method == 'post':
            response = requests.post('{}{}'.format(url, endpoint), json=data, params=params, headers=headers)
        else:
            response = requests.get('{}{}'.format(url, endpoint), params=params, headers=headers)

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
    organizations = read_organizations(FILE_PATH)

    results_dict = {}
    # save the organizations
    for organization in organizations:
        if organization["type"] == "CKAN":
            print("\n", organization["name"])
            results_dict[organization["name"]] = {}
            url = "{}/api/3/action/".format(organization["source"])

            code, result = ckan_api_request(url, 'package_search?facet.field=["tags", "groups"]&facet.limit=-1', "get", API_TOKEN)
            facets = result["result"]["search_facets"]
            tags = [(i["name"], i["count"]) for i in facets["tags"]["items"]]
            groups = [(i["name"], i["count"]) for i in facets["groups"]["items"]]
            print("TAGS ", len(tags), ' =>\t', tags)
            print("GROUPS ", len(groups), '=>\t', groups)
    return 0


if __name__ == '__main__':
    sys.exit(main())
