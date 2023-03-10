#!/usr/bin/env python
import json
import pprint
import csv
import io
import shutil
import os
import sys

# parameters from ENV
from dotenv import load_dotenv
load_dotenv('../.env')
ORGANIZATIONS_FILE_PATH = os.getenv('ORGANIZATION_LIST_PATH')
OUTPUT_FILE = "./data/output/dataset_list.csv"


def get_datasets_list(file_dir: str) -> list:
    # read the datasets files
    print(" - Read input dir: {}".format(file_dir))

    datasets = []

    for file in os.listdir(file_dir):
        if file.startswith("meta_") and file.endswith(".json"):
            file_path = os.path.join(file_dir, file)
            datasets += [file_path]

    return datasets


def read_dataset(file_path: str, organization: dict) -> dict:
    # read the dataset file and transform to json dict
    with open(file_path) as jsonfile:
        dataset = json.load(jsonfile)
    dataset["organization"] = organization["name"]
    dataset["tags"] = ""
    if dataset.get("theme"):
        dataset["tags"] = ",".join(dataset.get("theme", []))
    dataset.pop('resources', None)
    if organization["type"] == "OpenDataSoft":
        dataset["url"] = organization["source"] + "/explore/dataset/" + dataset["identifier"]
    else:
        dataset["url"] = organization["source"] + "/dataset/" + dataset["identifier"]
    return dataset


def read_organizations() -> dict:
    # read the dataset file and transform to json dict
    with open(ORGANIZATIONS_FILE_PATH) as jsonfile:
        organizations = json.load(jsonfile)["organizations"]
    organizations = {org["name"]: org for org in organizations}
    return organizations


def generate_csv(datsets: list, output_path: str) -> int:
    rows = []
    for dataset in datsets:
        columns = ["organization", "identifier", "url", "tags"]
        row = {k: dataset[k] for k in columns}
        rows += [row]

    with open(output_path, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, rows[0].keys())
        dict_writer.writeheader()
        dict_writer.writerows(rows)

    return len(rows)


def main() -> int:

    dir_dict = {
                    "valenciaOpenDataSoft": "valencia",
                    "datosAbiertosDipCas": "dipcas",
                    "datosAbiertosSagunto": "sagunto",
                    "dadesobertesGVA": "gva",
                    "dadesObertesSeu-eCat": "aoc",
                    "datosAbiertosTorrent": "torrent",
                    "openDataAlcoi": "alcoi"
                }

    if len(sys.argv) > 1:
        input_dir = sys.argv[1]
    else:
        print("* ERROR: No input dir")
        return -1

    print(" * Reading files from {}".format(input_dir))

    # get the organization data
    organizations = read_organizations()
    print(" * Got organizations ({}) data: {}".format(len(organizations),
                                                      ', '.join([org for org in organizations])))

    # process the datasets
    datasets = []

    for dir_name, org in dir_dict.items():
        # read the input file
        dir_path = os.path.join(input_dir, dir_name)
        dataset_files = get_datasets_list(dir_path)
        print("\t - Got {} datasets".format(len(dataset_files)))

        # save the organizations
        for dataset_file in dataset_files:
            dataset = read_dataset(dataset_file, organizations[org])
            datasets += [dataset]

    print(" \t => Retrieved {} datasets".format(len(datasets)))

    generate_csv(datasets, OUTPUT_FILE)

    return 0


if __name__ == '__main__':
    sys.exit(main())
