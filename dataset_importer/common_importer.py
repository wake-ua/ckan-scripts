#!/usr/bin/env python
import json
import os
import csv

from unidecode import unidecode

import dataset_importer_ckan
from scripts.commons import commons

# parameters from ENV
from dotenv import load_dotenv
load_dotenv('../.env')

CKAN_URL = os.getenv('CKAN_URL')
ORGANIZATIONS_FILE_PATH = os.getenv('ORGANIZATION_LIST_PATH')
DATASET_LIST_PATH = os.getenv('DATASET_LIST_PATH')
VOCABULARY_LIST_PATH = os.getenv('VOCABULARY_LIST_PATH')
TAG_LIST_PATH = os.getenv('TAG_LIST_PATH')

# parameters
LANGS = ['es', 'ca', 'en']


def read_dataset_list(file_path: str) -> dict:
    # read the groups file
    print(" - Read input file: {}".format(file_path))

    datasets = {}

    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for row in reader:
            datasets[row['id']] = row

    return datasets


def read_vocabulary(file_path: str) -> dict:
    # read the tags file
    print(" - Read input file: {}".format(file_path))

    tags = {}

    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for row in reader:
            key = unidecode(row["tag_vocabulary_es"].strip().lower())
            tags[key] = [row["tag_vocabulary_" + lang] + '-' + lang for lang in LANGS
                         if len(row["tag_vocabulary_" + lang].strip()) > 0]

    print(" \t => Read {} tags(s): {}".format(len(tags), tags.items()))

    return tags


def read_tags(file_path: str) -> dict:
    # read the tags file
    print(" - Read input file: {}".format(file_path))

    tags = {}

    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for row in reader:
            key = unidecode(row["tag_es"].strip().lower())
            tags[key] = [unidecode(row["tag_" + lang]).strip().lower() + '-' + lang for lang in LANGS
                         if len(row["tag_" + lang].strip()) > 0]

    print(" \t => Read {} free tags(s): {}".format(len(tags), tags.items()))

    return tags


def get_datasets_list(file_dir: str) -> list:

    # read the datasets files
    print(" - Read input dir: {}".format(file_dir))

    datasets = []

    for file in os.listdir(file_dir):
        if file.startswith("meta_") and file.endswith(".json"):
            file_path = os.path.join(file_dir, file)
            datasets += [file_path]

    return datasets


def read_organization(organization_name: str) -> dict:
    # read the dataset file and transform to json dict
    with open(ORGANIZATIONS_FILE_PATH) as jsonfile:
        organizations = json.load(jsonfile)["organizations"]
        for organization in organizations:
            if organization["name"] == organization_name:
                return organization
    return {}


def read_dataset(file_path: str) -> dict:
    # read the dataset file and transform to json dict
    with open(file_path) as jsonfile:
        dataset = json.load(jsonfile)
        dataset = dataset.get("result", dataset)
    return dataset


def get_translated_field(field: str, dataset: dict, default_value: str, org_name: str, mandatory: bool = False) -> dict:
    new_field = {'es': "", 'ca': "", "en": ""}

    for lang in new_field.keys():
        value = ""
        data = dataset.get(field, {})
        if isinstance(data, dict):
            value = data.get(lang, "")
        if not value:
            value = dataset.get(field + '_' + lang, "")
        if not value:
            if lang == "es":
                data = dataset.get(field, "")
                if isinstance(data, str):
                    value = data
                if not value:
                    value = default_value
            else:
                value = new_field["es"]
        if value:
            new_field[lang] = value

    # special cases
    if org_name == "valencia":
        multi_value = new_field["es"].split(" / ")
        if len(multi_value) == 2:
            new_field["ca"] = multi_value[0]
            new_field["es"] = multi_value[1]
            new_field["en"] = multi_value[1]

    if mandatory:
        langs = list(new_field.keys())
        some_value = dataset["id_portal"]
        for lang in langs:
            if len(new_field[lang]) > 0:
                some_value = new_field[lang]
                break
        for lang in langs:
            if len(new_field[lang]) == 0:
                new_field[lang] = some_value

    return new_field


def get_ckan_dataset(path: str, dataset: dict, organization: dict) -> dict:
    # map attributes to ckan dataset
    ckan_dataset = {
        "name": dataset["id_custom"] + "-" + organization["name"] + "-" + dataset["id_portal"],
        "title": get_translated_field("title", dataset, dataset["id_portal"], organization["name"], True),
        "notes": get_translated_field("description", dataset, dataset["title"], organization["name"]),
        "url": organization["source"] + dataset["id_portal"],
        "owner_org": organization["name"],
        "license_id": dataset.get("license", organization.get("license_id"))
    }

    # fix url for the INE
    # TODO fix licenses for INE
    if organization["name"] == "ine":
        ckan_dataset["url"] = "https://www.ine.es/jaxiT3/Tabla.htm?t=" + dataset["id_portal"].split('_')[-1]
        for lang, value in ckan_dataset["notes"].items():
            if ckan_dataset["notes"][lang].startswith(': '):
                ckan_dataset["notes"][lang] = ckan_dataset["notes"][lang][2:]


    # custom licenses
    if ckan_dataset["license_id"].lower().strip() == "http://www.opendefinition.org/licenses/cc-by":
        ckan_dataset["license_id"] = "cc-by"

    # spatial if existing
    if organization["spatial"]:
        ckan_dataset["spatial"] = json.dumps(organization["spatial"])

    # location
    if organization.get("territorio"):
        ckan_dataset["location"] = organization["territorio"]

    # original labels
    if dataset.get("theme"):
        ckan_dataset["original_tags"] = ", ".join(dataset.get("theme", []))

    # fix name if too long
    if len(ckan_dataset["name"]) > 100:
        print("WARNING: shortening name to <100")
        ckan_dataset["name"] = ckan_dataset["name"][0:100]

    # check resources
    ckan_resources = []
    resource_ids = get_resource_ids(ckan_dataset)

    res_num = 0
    for resource in dataset["resources"]:
        ckan_resource = {}

        # handle urls
        if isinstance(resource["downloadUrl"], str):
            ckan_resource["url"] = resource["downloadUrl"]
        else:
            ckan_resource["url"] = resource["downloadUrl"][0]
            for pair in zip(resource["downloadUrl"], resource["mediaType"]):
                url = pair[0]
                mimetype = pair[1]
                if mimetype.lower().strip() == 'csv':
                    ckan_resource["url"] = url
                    break

        if resource.get("name"):
            ckan_resource["name"] = {lang: resource.get("name", "") for lang in LANGS}
        # elif resource.get("path"):
        #     ckan_resource["name"] = {lang: resource.get("path", "").split('/')[-1].split('.')[0] for lang in LANGS}
        elif len(ckan_resource.get("url", "").split('/')[-1].split('.')) == 2:
            ckan_resource["name"] = {lang: ckan_resource.get("url", "").split('/')[-1].split('.')[0] for lang in LANGS}
        else:
            if len(dataset["resources"])>1:
                ckan_resource["name"] = {lang: dataset["id_portal"][0:80].rsplit('-', 1)[0] + '-file-' + str(res_num)
                                         for lang in LANGS}
            else:
                ckan_resource["name"] = {lang: dataset["id_portal"][0:80].rsplit('-', 1)[0] for lang in LANGS}
            print(ckan_resource["name"])

        # set resource id
        if resource_ids:
            ckan_resource["id"] = resource_ids[ckan_resource["url"]]

        ckan_resource["description"] = {lang: "" for lang in LANGS}

        ckan_resource["format"] = ckan_resource["url"].split('/')[-1]
        if ckan_resource["format"].find('.csv') >= 0:
            ckan_resource["format"] = 'csv'

        mimetype = ""
        if resource.get("path"):
            mimetype = resource.get("path", "").split('.')[-1]
        ckan_resource["mimetype"] = mimetype

        ckan_resources += [ckan_resource]
        res_num += 1

    if ckan_resources:
        ckan_dataset["resources"] = ckan_resources

    # specific additions for CKAN dataset
    if organization["type"] == "CKAN":
        print("\t - Complete metadata with specific CKAN info")
        ckan_dataset = dataset_importer_ckan.complete_ckan_dataset(path, organization, ckan_dataset)

    # prefix title with org name
    for lang in ckan_dataset["title"].keys():
        ckan_dataset["title"][lang] = organization["shortname"][lang] + ': ' + ckan_dataset["title"][lang]

    return ckan_dataset


def import_dataset(ckan_dataset: dict, update: bool = False) -> (int, dict):

    # call the endpoint
    if not update:
        success, result = commons.ckan_api_request(endpoint="package_create", method="post", data=ckan_dataset)
    else:
        ckan_dataset["id"] = ckan_dataset["name"]
        success, result = commons.ckan_api_request(endpoint="package_patch", method="post", data=ckan_dataset)
    return success, result


def delete_dataset(dataset: dict) -> (int, dict):

    return commons.ckan_api_request('package_delete', 'post', {'id': dataset["name"]})


def get_resource_ids(dataset: dict) -> dict:
    resource_ids = {}

    success, result = commons.ckan_api_request(endpoint="package_show", method="get",
                                               params={"id": dataset["name"]}, verbose=False)
    if success >= 0:
        ckan_dataset = result["result"]
        for resource in ckan_dataset.get("resources", []):
            resource_ids[resource["url"]] = resource["id"]

    return resource_ids


def dataset_has_resource_ids(ckan_dataset: dict) -> bool:
    for resource in ckan_dataset["resources"]:
        if resource.get('id'):
            return True
    return False


# groups,groups_extra,vocabulary,vocabulary_extra,tags
def add_semantics(dataset: dict, dataset_data: dict, vocabulary_data: dict, tags_data: dict) -> dict:
    # add groups
    group_ids = [group.lower().strip() for group in (dataset_data['groups'] + ','
                                                     + dataset_data['groups_extra']).split(',') if len(group) > 1]
    if group_ids:
        print("\t\t - Adding groups to dataset {}: {}".format(dataset['name'], ", ".join(group_ids)))
        dataset["groups"] = [{"name": group} for group in set(group_ids) if len(group) > 1]

    # add vocabulary
    vocabulary_tags = [unidecode(tag.lower().strip()) for tag in (dataset_data['vocabulary'] + ','
                                                                  + dataset_data['vocabulary_extra']).split(',')
                       if len(tag) > 1]
    if vocabulary_tags:
        print("\t\t - Adding vocabulary tags to dataset {}: {}".format(dataset['name'], ", ".join(vocabulary_tags)))
        vocabulary_tag_list = []
        for tag in vocabulary_tags:
            vocabulary_tag_list += vocabulary_data[tag]
        dataset['tag_string_schemaorg'] = ','.join(vocabulary_tag_list)

    # add free tags
    tags = [unidecode(tag.lower().strip()) for tag in (dataset_data['tags']).split(',') if len(tag) > 1]
    if tags:
        print("\t\t - Adding free tags to dataset {}: {}".format(dataset['name'], ", ".join(tags)))
        free_tag_list = []
        for tag in tags:
            new_tags = tags_data.get(tag)
            if new_tags:
                free_tag_list += new_tags
            else:
                free_tag_list += [tag + '-es']
                print("*ERROR* MISSING TAG: {}".format(tag))
                tags_data = read_tags(TAG_LIST_PATH)
        dataset['tag_string'] = ','.join(free_tag_list)

    return dataset


def import_datasets(input_dir: str, organization_name: str, selected_package: str = None) -> (int, dict):
    print("* Importing data for {} from {}".format(organization_name, input_dir))

    if selected_package:
        print(" => Importing ONLY selected package: {}".format(selected_package))

    # get the organization data
    organization = read_organization(organization_name)
    print(" * Got organization {} ({}) data".format(organization["title"]["en"], organization['name']))

    # get the dataset data
    dataset_master = read_dataset_list(DATASET_LIST_PATH)
    print(" * Got dataset master list {}:\n\t - Read {} datasets".format(DATASET_LIST_PATH, len(dataset_master)))

    # get tags and vocabulary
    vocabulary = read_vocabulary(VOCABULARY_LIST_PATH)
    tags = read_tags(TAG_LIST_PATH)

    # process the datasets
    created_datasets = []
    updated_datasets = []
    deleted_datasets = []

    # read the input file
    dataset_files = get_datasets_list(input_dir)
    print("\t - Found {} dataset files".format(len(dataset_files)))

    # save the organizations
    for dataset_file in dataset_files:
        dataset = read_dataset(dataset_file)

        if selected_package and dataset["id_portal"] != selected_package:
            continue

        print("\n* Reading dataset {}".format(dataset_file))
        print("\t - Data: {} {}".format(dataset["id_portal"], dataset["id_custom"]))
        ckan_dataset = get_ckan_dataset(dataset_file, dataset, organization)

        existing = dataset_has_resource_ids(ckan_dataset)

        if dataset_master[ckan_dataset['name']]['ok'] == '0':
            if existing:
                success, result = delete_dataset(ckan_dataset)
            else:
                print("\t => Skipping import: {} {}".format(organization, ckan_dataset['name']))
                continue
        else:
            ckan_dataset = add_semantics(ckan_dataset, dataset_master[ckan_dataset['name']], vocabulary, tags)
            success, result = import_dataset(ckan_dataset, update=existing)

        if success >= 0:
            if existing:
                if dataset_master[ckan_dataset['name']]['ok'] == '0':
                    print("\t * Deleted: {}...".format(str(result)[:500]))
                    deleted_datasets += [ckan_dataset['name']]
                else:
                    print("\t * Updated: {}...".format(str(result)[:500]))
                    updated_datasets += [result["result"]["name"]]
            else:
                print("\t * Created: {}...".format(str(result)[:500]))
                created_datasets += [result["result"]["name"]]
        else:
            print("\t => * Import Failed * existing?", existing)
            return -1

    print(" \t - Created {} datasets: {} \n\t - Updated {} datasets: {}  \n\t - Deleted {} datasets: {}".format(
        len(created_datasets), ', '.join([CKAN_URL + "/dataset/" + dataset for dataset in created_datasets]),
        len(updated_datasets), ', '.join([CKAN_URL + "/dataset/" + dataset for dataset in updated_datasets]),
        len(deleted_datasets), ', '.join([CKAN_URL + "/dataset/" + dataset for dataset in deleted_datasets])))
    return 0, {}
