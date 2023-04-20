#!/usr/bin/env python

import csv
import os
import sys
from scripts.commons import commons

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

CKAN_URL = os.getenv('CKAN_URL')
FILE_PATH = os.getenv('VOCABULARY_LIST_PATH')

# parameters
VOCABULARY_NAME = "schemaorg"
LANGS = ["es", "ca", "en"]


def read_tags(file_path: str) -> list:
    # read the tags file
    print(" - Read input file: {}".format(file_path))

    tags = []

    with open(FILE_PATH) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for row in reader:
            new_row = {}

            for field, value in row.items():
                if field.rsplit('_', 1)[-1] in LANGS:
                    parent_field = field.rsplit('_', 1)[0].strip()
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


def edit_vocabulary(name: str, tags: list, update: bool = False) -> (int, dict):
    # map attributes to ckan tag
    ckan_vocabulary = {
        "name": name,
        "tags": [{"name": tag["tag_vocabulary"][lang].strip().replace("'", " ").replace("’", " ") + "-" + lang} for tag in tags for lang in LANGS]
    }

    # call the endpoint
    if not update:
        success, result = commons.ckan_api_request(endpoint="vocabulary_create", method="post", data=ckan_vocabulary)
    else:
        ckan_vocabulary["id"] = name
        success, result = commons.ckan_api_request(endpoint="vocabulary_update", method="post", data=ckan_vocabulary)
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


if __name__ == '__main__':
    sys.exit(main())
