#!/usr/bin/env python
import requests
from requests.exceptions import HTTPError
import json
import pprint
import csv
import shutil
import os

# parameters from ENV
from dotenv import load_dotenv
load_dotenv('../.env')

UPLOADS_PATH = os.getenv('GROUP_IMAGE_UPLOADS_PATH')
API_TOKEN = os.getenv('API_TOKEN')
CKAN_URL = os.getenv('CKAN_URL')

# parameters
FILE_PATH = "./data/organizations_list.csv"
IMAGE_DIR = "./data/image"

CKAN_API_URL = "{}//api/3/action/".format(CKAN_URL)
CKAN_UPLOADS_URL = "{}/uploads/group/".format(CKAN_URL)

# read the organizations file
print(" - Read input file: {}".format(FILE_PATH))

organizations = []

with open(FILE_PATH) as csvfile:

    reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

    for row in reader:
        organizations += [row]
        print("\t * {}".format(row))

print(" \t => Read {} organization(s): {}".format(len(organizations), ', '.join([org['name'] for org in organizations])))

# prepare the data for import
headers = {'Authorization': API_TOKEN}

for org in organizations:
    print("\n * Create ORG: {}".format(org["name"]))
    ckan_org = {
                    "name": org["name"],
                    "display_name": org["title"],
                    "description": org["description"],
                    "image_display_url": "{}/{}".format(CKAN_UPLOADS_URL, org["image"]),
                    "image_url": "{}".format(org["image"])
                }

    # copy image to the uploads
    shutil.copyfile("{}/{}".format(IMAGE_DIR, org["image"]), "{}/{}".format(UPLOADS_PATH, org["image"]))

    # do the import
    try:
        response = requests.post('{}organization_create'.format(CKAN_API_URL), data=ckan_org, headers=headers)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        organization_dict = response.json().get('result', {})
        print("\t * Created: {}".format(organization_dict))

    except HTTPError as http_err:
        print(f'\t HTTP error occurred: {http_err} {response.json().get("error")}')  # Python 3.6
    except Exception as err:
        print(f'\t Other error occurred: {err}')  # Python 3.6
    else:
        print('\t Success!')


# Make the HTTP request to retrieve all organizations.
try:
    response = requests.get('{}organization_list'.format(CKAN_API_URL), headers=headers)

    # If the response was successful, no Exception will be raised
    response.raise_for_status()
    created_organizations_list = response.json().get('result', {})
    print(" - Got total {} organizations: {}".format(len(created_organizations_list),
                                                     ', '.join(created_organizations_list)))

except HTTPError as http_err:
    print(f'\t HTTP error occurred: {http_err} {response.json().get("error")}')  # Python 3.6
except Exception as err:
    print(f'\t Other error occurred: {err}')  # Python 3.6
else:
    print('\t Success!')

for org_name in created_organizations_list:
    payload = {"id": org_name}
    try:
        response = requests.get('{}organization_show'.format(CKAN_API_URL), params=payload, headers=headers)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        created_organization = response.json().get('result', {})
        print("\t * Got organization {}: {}".format(created_organization["name"], created_organization))

    except HTTPError as http_err:
        print(f'\t HTTP error occurred: {http_err} {response.json().get("error")}')  # Python 3.6
    except Exception as err:
        print(f'\t Other error occurred: {err}')  # Python 3.6
    else:
        print('\t Success!')

