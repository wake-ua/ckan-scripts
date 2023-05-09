#!/usr/bin/env python
import csv
import shutil
import os
import sys
from scripts.commons import commons

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

UPLOADS_PATH = os.getenv('GROUP_IMAGE_UPLOADS_PATH')
IMAGE_DIR = os.getenv('IMAGES_PATH')
API_TOKEN = os.getenv('API_TOKEN')
CKAN_URL = os.getenv('CKAN_URL')
FILE_PATH = os.getenv('GROUP_LIST_PATH')

CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)
CKAN_UPLOADS_URL = "{}/uploads/group/".format(CKAN_URL)


def edit_group(group: dict, update: bool = False) -> (int, dict):
    # map attributes to ckan group
    group_image_name = "logo_{}.png".format(group["name"])
    ckan_group = {
        "name": group["name"],
        "title": {lang: group["title" + "_" + lang] for lang in ["es", "ca", "en"]},
        "description": {lang: group["description" + "_" + lang] for lang in ["es", "ca", "en"]},
        "image_display_url": "{}/{}".format(CKAN_UPLOADS_URL, group_image_name),
        "image_url": group_image_name
    }

    # copy image to the uploads
    shutil.copyfile(os.path.join(IMAGE_DIR, group_image_name), os.path.join(UPLOADS_PATH, group_image_name))

    # call the endpoint
    if not update:
        success, result = commons.ckan_api_request(endpoint="group_create", method="post", data=ckan_group)
    else:
        ckan_group["id"] = group["name"]
        success, result = commons.ckan_api_request(endpoint="group_patch", method="post", data=ckan_group)
    return success, result


def main() -> int:
    created_groups = []
    updated_groups = []

    # read the input file
    groups = commons.read_groups(FILE_PATH)

    # save the groups
    for group in groups:
        print("\n * Creating group: {}".format(group["name"]))
        success, result = edit_group(group)
        if success >= 0:
            print("\t * Created: {}".format(str(result)[0:500]))
            created_groups += [group["name"]]
        else:
            print("\t => Created Failed, trying UPDATE...")
            success, result = edit_group(group, update=True)
            if success >= 0:
                print("\t * Updated: {}".format(str(result)[0:500]))
                updated_groups += [group["name"]]
            else:
                print("\t => * Update Failed *")
                return -1

    print(" * Finished: \n\t - Created {} groups: {} "
          "\n\t - Updated {} groups: {}".format(len(created_groups), ', '.join(created_groups),
                                                len(updated_groups), ', '.join(updated_groups)))

    success, total_groups = commons.ckan_api_request(endpoint="group_list", method="get")
    if success >= 0:
        print("\nCKAN groups: {}".format(', '.join(total_groups["result"])))
    else:
        print("\t => * Retrieving All groups Failed *")
        return -1

    return 0


if __name__ == '__main__':
    sys.exit(main())
