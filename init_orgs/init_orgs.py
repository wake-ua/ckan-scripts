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
FILE_PATH = os.getenv('ORGANIZATION_LIST_PATH')

# parameters
IMAGE_DIR = "./data/image"

CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)
CKAN_UPLOADS_URL = "{}/uploads/group/".format(CKAN_URL)


def read_organizations(file_path: str) -> list:
    # read the organizations file
    print(" - Read input file: {}".format(file_path))

    organizations = []

    with open(FILE_PATH) as jsonfile:
        organizations = json.load(jsonfile)["organizations"]

    print(" \t => Read {} organization(s): {}".format(len(organizations),
          ', '.join([org['name'] for org in organizations])))

    return organizations


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


def edit_organization(org: dict, update: bool = False) -> (int, dict):
    # map attributes to ckan org
    ckan_org = {
        "name": org["name"],
        "title": org["title"],
        "description": org["description"],
        "image_display_url": "{}/{}".format(CKAN_UPLOADS_URL, org["image"]),
        "image_url": "{}".format(org["image"]),
        "source": org.get("source", "")
    }

    # copy image to the uploads
    shutil.copyfile("{}/{}".format(IMAGE_DIR, org["image"]), "{}/{}".format(UPLOADS_PATH, org["image"]))

    # call the endpoint
    if not update:
        success, result = ckan_api_request(endpoint="organization_create", method="post", token=API_TOKEN,
                                           data=ckan_org)
    else:
        ckan_org["id"] = org["name"]
        success, result = ckan_api_request(endpoint="organization_patch", method="post",
                                           token=API_TOKEN, data=ckan_org)
    return success, result


def main() -> int:
    created_orgs = []
    updated_orgs = []

    # read the input file
    organizations = read_organizations(FILE_PATH)

    # save the organizations
    for org in organizations:
        print("\n * Creating ORG: {}".format(org["name"]))
        success, result = edit_organization(org)
        if success >= 0:
            print("\t * Created: {}".format(result))
            created_orgs += [org["name"]]
        else:
            print("\t => Created Failed, trying UPDATE...")
            success, result = edit_organization(org, update=True)
            if success >= 0:
                print("\t * Updated: {}".format(result))
                updated_orgs += [org["name"]]
            else:
                print("\t => * Update Failed *")
                return -1

    print(" * Finished: \n\t - Created {} organizations: {} "
          "\n\t - Updated {} organizations: {}".format(len(created_orgs), ', '.join(created_orgs),
                                                     len(updated_orgs), ', '.join(updated_orgs)))

    success, total_orgs = ckan_api_request(endpoint="organization_list", method="get", token=API_TOKEN)
    if success >= 0:
        print("\nCKAN Organizations: {}".format(', '.join(total_orgs["result"])))
    else:
        print("\t => * Retrieving All Organizations Failed *")
        return -1

    return 0


if __name__ == '__main__':
    sys.exit(main())
