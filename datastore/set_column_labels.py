#!/usr/bin/env python
import csv
import json
import os
import sys
import requests
from requests.exceptions import HTTPError
from unidecode import unidecode
from scripts.commons import commons

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

API_TOKEN = os.getenv('API_TOKEN')
CKAN_URL = os.getenv('CKAN_URL')
FILE_PATH = os.getenv('COLUMN_LIST_PATH')
ONTOLOGY_PATH = os.getenv('ONTOLOGY_LIST_PATH')

# parameters
DO_OUTPUT_LIST = True
OUTPUT_PATH = "./output/datastore_column_info.csv"


def read_labels(file_path: str) -> dict:
    # read the tags file
    print(" - Read input file: {}".format(file_path))

    column_label_dict = {}
    labels = []

    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for row in reader:
            labels += [row['label']]
            for column in row['synonyms'].split(','):
                column_new = unidecode(column.strip().lower())
                if column_label_dict.get(column_new):
                    raise Exception('Duplicate key on label match {}'.format(column_new))
                column_label_dict[column_new] = row

    print(" \t => Read {} labels(s): {}".format(len(labels), ', '.join(labels)))

    return column_label_dict


def read_ontology(file_path: str) -> dict:
    # organization, package_id, resource_id, column, ontology, predicate, function, comments
    # read the tags file
    print(" - Read ontology input file: {}".format(file_path))

    ontology_dict = {}

    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for row in reader:
            resource_id = row['resource_url'].split('/resource/')[1]
            if CKAN_URL == "http://127.0.0.1:5000":
                resource_id = row['resource_id_local']
            package_id = row['resource_url'].split('/dataset/')[1].split('/resource/')[0]
            key = (row['organization'], package_id, resource_id)
            ontology = ontology_dict.get(key, {})
            ontology_column = ontology.get(row['column'], [])
            ontology_column += [{k: row[k] for k in ['ontology', 'predicate', 'function']}]
            ontology[row['column']] = ontology_column
            ontology_dict[key] = ontology

    print(" \t => Read {} ontology entries".format(len(ontology_dict)))

    return ontology_dict


def get_resources_list() -> (int, list):
    success, result = commons.ckan_api_request("wakeua_list_datastore_resources", "get")
    if success >= 0:
        return success, result['result']
    return success, result


def check_resource(resource_id: str) -> (int, list):
    success, result = commons.ckan_api_request("resource_show", "get", params={"id": resource_id})
    if success < 0:
        if result["code"] == 404:
            print("ERROR: NOT FOUND \t" + resource_id)
            print(result["http_error"].errno)
            raise Exception("ERROR: Found deleted resource", resource_id)
        else:
            raise Exception("ERROR: Unknown error check resource", resource_id, success, result)

    return success, result


def get_datastore_info(resource_ids: list, raw: bool = False) -> list:
    datastore_info = []
    i = 1
    for resource_id in resource_ids:
        if not raw:
            print("{}/{}\t".format(i, len(resource_ids)), resource_id)

        success, result = check_resource(resource_id)
        resource_data = result['result']

        if not raw:
            print('\t', resource_data['package_id'], resource_data['name']['es'])

        success, result = commons.ckan_api_request("package_show", "get", params={"id": resource_data['package_id']})
        package_data = result['result']

        success, result = commons.ckan_api_request("datastore_search", "post",
                                                   data={"resource_id": resource_id, "limit": 1})
        if success < 0:
            raise Exception("ERROR: Cannot find dataset", resource_id)
        if raw:
            raw_dict = result['result']
            raw_dict['package_data'] = package_data
            raw_dict['resource_data'] = resource_data
            datastore_info += [raw_dict]
        else:
            for field in result['result']['fields']:

                field_row = {
                                'id': field.get('id', ''),
                                'type': field.get('type', ''),
                                'label': field.get('info', {}).get('label', ''),
                                'notes': field.get('info', {}).get('notes', ''),
                                'type_override':  field.get('info', {}).get('type_override', ''),
                                'example': '',
                                'resource': resource_data["url"],
                                'dataset_id ': package_data['name'],
                                'dataset ': package_data['url'],
                                'tags': [t["name"] for t in package_data["tags"] if t["name"].endswith("-es")],
                                'vocabulary': package_data.get("tag_string_schemaorg"),
                                'groups': [g["name"] for g in package_data["groups"]],
                                'organization': package_data["organization"]["name"]
                }
                if len(result['result']['records']) > 0:
                    field_row['example'] = result['result']['records'][0].get(field.get('id', ''))

                datastore_info += [field_row]
        i += 1
    return datastore_info


def set_column_labels(resource_ids: list, labels_dict: dict, ontology_dict: dict) -> int:
    count = 0
    for resource_id in resource_ids:
        datastore_info = get_datastore_info([resource_id], raw=True)[0]
        fields = datastore_info['fields'][1:]
        index = 1
        form_data = {}
        update = False

        organization = datastore_info['package_data']['organization']['name']
        package_id = datastore_info['package_data']['name']
        key_ontology = (organization, package_id, resource_id)
        ontology_info = ontology_dict.get(key_ontology, {})

        for field in fields:
            form_data["info__{}__type_override".format(index)] = field.get('info',{}).get('type_override', "")
            form_data["info__{}__label".format(index)] = field.get('info', {}).get('label', "")
            form_data["info__{}__notes".format(index)] = field.get('info', {}).get('notes', "")
            form_data["info__{}__ontology".format(index)] = field.get('info', {}).get('ontology', "")

            base_id = unidecode(field['id'].strip().lower())
            base_id_alt = base_id.replace(' ', '_')

            if labels_dict.get(base_id, labels_dict.get(base_id_alt)):
                if not update:
                    print("\n {}/dataset/{}/resource/{} ".format(CKAN_URL, datastore_info['package_data']['name'],
                                                                 resource_id))
                ref_info = labels_dict.get(base_id, labels_dict.get(base_id_alt))
                print('\t - Match: {} ({}) => {} '.format(field['id'], base_id, ref_info['label']))
                form_data["info__{}__label".format(index)] = ref_info['label']
                form_data["info__{}__notes".format(index)] = ref_info['description']
                update = True

            if ontology_info.get(field['id']):
                if not update:
                    print("\n {}/dataset/{}/resource/{} ".format(CKAN_URL, datastore_info['package_data']['name'],
                                                                 resource_id))
                ontology_list = ontology_info.get(field['id'])
                print('\t - Match Ontology: {} => {} '.format(field['id'],
                      ', '.join([o['predicate'] for o in ontology_list])))
                form_data["info__{}__ontology".format(index)] = json.dumps(ontology_list)
                update = True

            index += 1

        if update:
            count += 1
            # set headers
            headers = {'Authorization': API_TOKEN}

            # do the actual call
            response = {}
            try:
                response = requests.post('{}/dataset/{}/dictionary/{}'.format(CKAN_URL,
                                         datastore_info['package_data']['name'],
                                         resource_id), data=form_data,headers=headers)

                # If the response was successful, no Exception will be raised
                response.raise_for_status()

            except HTTPError as http_err:
                if response:
                    print(f'\t HTTP error occurred: {http_err} {response.json().get("error")}')  # Python 3.6
                else:
                    print(f'\t HTTP error occurred: {http_err}, no response')  # Python 3.6
                raise http_err
            except Exception as err:
                print(f'\t Other error occurred: {err}')  # Python 3.6
                raise err

    return count


def main() -> int:
    labels_dict = read_labels(FILE_PATH)
    ontology_dict = read_ontology(ONTOLOGY_PATH)

    if len(sys.argv) > 1:
        if len(sys.argv[1].split(',')) > 1:
            resource_ids = sys.argv[1].split(',')
        else:
            resource_ids = [sys.argv[1]]
    else:
        # get_all_resources_ids_in_datastore
        success, result = get_resources_list()
        resource_ids = result

    # add labels
    matches = set_column_labels(resource_ids, labels_dict, ontology_dict)
    print(" * DONE: {} resources updated".format(matches))

    if DO_OUTPUT_LIST:
        # gather datasets columns
        datastore_info = get_datastore_info(resource_ids)

        with open(OUTPUT_PATH, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=list(datastore_info[0].keys()))
            writer.writeheader()
            writer.writerows(datastore_info)

        print("DONE: Written {} rows to {}".format(len(datastore_info), OUTPUT_PATH))

    return 0


if __name__ == '__main__':
    sys.exit(main())
