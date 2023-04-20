#!/usr/bin/env python
import json
import shutil
import os
import sys
from scripts.commons import commons

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

UPLOADS_PATH = os.getenv('GROUP_IMAGE_UPLOADS_PATH')
FILE_PATH = os.getenv('ORGANIZATION_LIST_PATH')
IMAGE_DIR = os.getenv('IMAGES_PATH')
CKAN_URL = os.getenv('CKAN_URL')

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
    shutil.copyfile(os.path.join(IMAGE_DIR, org["image"]),os.path.join(UPLOADS_PATH, org["image"]))

    # call the endpoint
    if not update:
        success, result = commons.ckan_api_request(endpoint="organization_create", method="post", data=ckan_org)
    else:
        ckan_org["id"] = org["name"]
        success, result = commons.ckan_api_request(endpoint="organization_patch", method="post", data=ckan_org)
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
            print("\t * Created: {}".format(str(result)[0:500]))
            created_orgs += [org["name"]]
        else:
            print("\t => Created Failed, trying UPDATE...")
            success, result = edit_organization(org, update=True)
            if success >= 0:
                print("\t * Updated: {}".format(str(result)[0:500]))
                updated_orgs += [org["name"]]
            else:
                print("\t => * Update Failed *")
                return -1

    print(" * Finished: \n\t - Created {} organizations: {} "
          "\n\t - Updated {} organizations: {}".format(len(created_orgs), ', '.join(created_orgs),
                                                       len(updated_orgs), ', '.join(updated_orgs)))

    success, total_orgs = commons.ckan_api_request(endpoint="organization_list", method="get")
    if success >= 0:
        print("\nCKAN Organizations: {}".format(', '.join(total_orgs["result"])))
    else:
        print("\t => * Retrieving All Organizations Failed *")
        return -1

    return 0


if __name__ == '__main__':
    sys.exit(main())
