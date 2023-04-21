#!/usr/bin/env python   §§§
import os
import sys
from common_importer import import_datasets

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

DATASETS_PATH = os.getenv('DATASETS_PATH')

# global variables and default values
ORG_DIR = {

    # CKAN
    "opendata.alcoi.org": "alcoi",
    "datosabiertos.torrent.es": "torrent",
    "datosabiertos.sagunto.es": "sagunto",
    "dadesobertes.gva.es": "gva",

    # OpenDataSoft
    "valencia.opendatasoft.com": "valencia",
    "datosabiertos.dipcas.es": "dipcas",

    # INE
    "servicios.ine.es": "ine"
}


def main() -> int:

    # input parameters
    selected_package = None

    if len(sys.argv) > 1:
        input_dirs = [sys.argv[1]]
        if len(sys.argv) > 2:
            selected_package = sys.argv[2]
    else:
        input_dirs = [os.path.join(DATASETS_PATH, subdir) for subdir in ORG_DIR.keys()]

    for input_dir in input_dirs:
        org = ORG_DIR[input_dir.rsplit('/', 1)[-1]]
        print(org)
        import_datasets(input_dir, org, selected_package)
    return 0


if __name__ == '__main__':
    sys.exit(main())


# def add_datasets(tags: list) -> (int, dict):
#     # maping with the indexes
#     success, result = commons.ckan_api_request(endpoint="package_list", method="get", token=API_TOKEN)
#     package_list = result["result"]
#
#     cv_field = 'tag_string_schemaorg'
#     not_found = []
#     datasets = {}
#     for tag in tags:
#         tag_datasets = [dataset.strip() for dataset in tag["datasets"].split(" ") if dataset]
#         for dataset_name in tag_datasets:
#             matches = [p for p in package_list if dataset_name.startswith(p.split('-', 1)[-1])
#                           and len(dataset_name) >= len(p.split('-', 1)[-1])]
#
#             if len(matches) == 0:
#                 not_found += [dataset_name]
#                 continue
#
#             dataset_id = matches[0]
#
#             datasets[dataset_name] = datasets.get(dataset_name, {"id": dataset_id})
#             datasets[dataset_name][cv_field] = datasets[dataset_name].get(cv_field, []) + \
#                                                [v.strip() + "-" + k for k, v in tag["tag_vocabulary"].items()]
#             for lang, tag_strings in tag["tag"].items():
#                 datasets[dataset_name]["tag_string"] = datasets[dataset_name].get("tag_string", []) + \
#                                        [t.strip() + "-" + lang for t in tag_strings.split(' ') if t.strip()]
#
#     for name, dataset in datasets.items():
#         dataset[cv_field] = ','.join(dataset[cv_field])
#         dataset["tag_string"] = ','.join(dataset["tag_string"])
#
#         print("\t\t - Adding CV tags to dataset {}: {}".format(name, dataset[cv_field]))
#         print("\t\t - Adding free tags to dataset {}: {}".format(name, dataset["tag_string"]))
#         success, result = ckan_api_request(endpoint="package_patch", method="post",
#                                            token=API_TOKEN, data=dataset)
#         if success < 0:
#             if result['http_error'].response.status_code == 404:
#                 not_found += [name]
#             else:
#                 raise Exception("Could not patch dataset {}: ".format(name) + str(result))
#         print(result)
#
#     print(" ** WARNING NOT FOUND: {}".format(', '.join(set(not_found))))
#
#     return success, result
# def add_datasets_groups(tags: list) -> (int, dict):
#     # maping with the indexes
#     success, result = commons.ckan_api_request(endpoint="package_list", method="get")
#     package_list = result["result"]
#
#     datasets = {}
#     not_found = []
#
#     for tag in tags:
#         group = tag["group"]
#         group_datasets = [dataset.strip() for dataset in tag["datasets"].split(" ") if dataset]
#         for dataset_name in group_datasets:
#             matches = [p for p in package_list if dataset_name.startswith(p.split('-', 1)[-1])
#                           and len(dataset_name) >= len(p.split('-', 1)[-1])]
#
#             if len(matches) == 0:
#                 not_found += [dataset_name]
#                 continue
#
#             dataset_id = matches[0]
#
#             datasets[dataset_id] = datasets.get(dataset_id, []) + [group]
#
#     for dataset, group_ids in datasets.items():
#         name = dataset
#
#         print("\t\t - Adding groups to dataset {}: {}".format(name, ", ".join(group_ids)))
#         ckan_dataset = {"id": name, "groups": [{"name": group} for group in set(group_ids)]}
#
#         success, result = commons.ckan_api_request(endpoint="package_patch", method="post", data=ckan_dataset)
#         if success < 0:
#             if result['http_error'].response.status_code == 404:
#                 not_found += [name]
#             else:
#                 raise Exception("Could not patch dataset {}: ".format(name) + str(result))
#         print(result)
#
#     print(" ** WARNING NOT FOUND: {}".format(', '.join(set(not_found))))
#
#     return success, result

