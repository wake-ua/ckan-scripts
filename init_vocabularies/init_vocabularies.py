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


def edit_vocabulary(name: str, tags: list, update: bool = False) -> (int, dict):
    # map attributes to ckan tag
    ckan_vocabulary = {
        "name": name,
        "tags": [{"name": tag["tag_vocabulary"][lang].strip().replace("'", " ").replace("’", " ") + "-" + lang} for tag in tags for lang in commons.LANGS]
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
    tags = commons.read_vocabulary(FILE_PATH)
    print(" \t -> Read {} vocabulary items: {}".format(len(tags), ', '.join([tag['tag_vocabulary']["es"] for tag in tags])))

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
