#!/usr/bin/env python
import os
import sys
import csv
from scripts.commons import commons


# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

CKAN_URL = os.getenv('CKAN_URL')

# parameters
CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)
OUTPUT_PATH = "./output/dataset_list.csv"


def save_datasets_list() -> int:
    count = 0
    writer = None
    step = 500

    success, result = commons.ckan_api_request("organization_list", "get")
    organizations = result['result']

    with open(OUTPUT_PATH, 'w') as f:
        for organization in organizations:
            print("\n * Organization", organization)
            params = {"q": "organization:{}".format(organization), "rows": step}
            success, result = commons.ckan_api_request("package_search", "get", params=params)
            if success < 0:
                raise("ERROR: Cannot retrieve datasets", organization)
            total = result['result']['count']
            datasets = result['result']['results']

            while len(datasets) < total and len(result['result']['results']) > 0:
                params = {"q": "organization:{}".format(organization), "rows": step, "start": len(datasets)}
                success, result = commons.ckan_api_request("package_search", "get", params=params)
                if success < 0:
                    raise ("ERROR: Cannot retrieve datasets")
                datasets += result['result']['results']

            print("\t => Total", len(datasets))

            count += len(datasets)

            for dataset in datasets:
                row = {
                    'ok': '',
                    'organization': dataset["organization"]["name"],
                    'id': dataset.get('name', ''),
                    'title': dataset.get('title', {}).get('es', ''),
                    'url': dataset.get('url'),
                    'tags': '"'
                            + ','.join([t["name"][:-3] for t in dataset["tags"] if t["name"].endswith("-es")])
                            + '"',
                    'vocabulary': '"'
                            + ','.join([t[:-3] for t in dataset.get("tag_string_schemaorg", "").split(',') if t.endswith("-es")])
                            + '"',
                    'groups': '"'
                            + ','.join([g["name"] for g in dataset["groups"]])
                            + '"',
                    'original_tags': dataset.get("original_tags",'')
                }
                # if len(row['groups'])>2:
                #     row['ok'] = 1
                print(row.values())

                if not writer:
                    writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                    writer.writeheader()
                writer.writerows([row])

    return count


def main() -> int:

    # save in the csv all te datasets
    count = save_datasets_list()
    print("Total:", count)

    return 0


if __name__ == '__main__':
    sys.exit(main())
