#!/usr/bin/env python
import requests
from requests.exceptions import HTTPError
import csv
import os
import sys
from unidecode import unidecode

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

API_TOKEN = os.getenv('API_TOKEN')
CKAN_URL = os.getenv('CKAN_URL')
FILE_PATH = os.getenv('COLUMN_LIST_PATH')

# parameters
CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)
OUTPUT_PATH = "./output/datastore_column_info.csv"


def read_labels(file_path: str) -> dict:
    # read the tags file
    print(" - Read input file: {}".format(file_path))

    column_label_dict = {}
    labels = []

    with open(FILE_PATH) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for row in reader:
            labels += [row['label']]
            for column in row['columns'].split(','):
                column_new = unidecode(column.strip().lower())
                if column_label_dict.get(column_new):
                    raise Exception('Duplicate key on label match {}'.format(column_new))
                column_label_dict[column_new] = row

    print(" \t => Read {} labels(s): {}".format(len(labels), ', '.join(labels)))

    return column_label_dict


def ckan_api_request(endpoint: str, method: str, token: str, data: dict = {}, params: dict = {}) -> (int, dict):
    # set headers
    headers = {'Authorization': token}

    result = {}

    # do the actual call
    try:
        if method == 'post':
            response = requests.post('{}{}'.format(CKAN_API_URL, endpoint), json=data, params=params, headers=headers)
        else:
            response = requests.get('{}{}'.format(CKAN_API_URL, endpoint), params=params, headers=headers)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        result = response.json()
        return 0, result

    except HTTPError as http_err:
        print(f'\t HTTP error occurred: {http_err} {response.json().get("error")}')  # Python 3.6
        result = {"code": response.status_code, "http_error": http_err, "error": response.json().get("error")}
    except Exception as err:
        print(f'\t Other error occurred: {err}')  # Python 3.6
        result = {"error": err}

    return -1, result


def get_resources_list() -> (int, list):
    success, result = ckan_api_request("wakeua_list_datastore_resources", "get", API_TOKEN)
    if success >= 0:
        return success, result['result']
    return success, result


def delete_datastore_resource(resource_id: str) -> (int, dict):
    print("\t * DELETE *", resource_id)
    success, result = ckan_api_request("datastore_delete", "post", API_TOKEN,
                                       data={"resource_id": resource_id, 'force': True})
    if success >= 0:
        return success, result['result']
    return success, result


def check_resource(resource_id: str) -> (int, list):
    success, result = ckan_api_request("resource_show", "get", API_TOKEN, params={"id": resource_id})
    if success < 0:
        if result["code"] == 404:
            print("ERROR: NOT FOUND \t" + resource_id)
            print(result["http_error"].errno)
            success, result = delete_datastore_resource(resource_id)
            print(" => DELETED resource: " + resource_id, success, result)
            raise Exception("ERROR: Found deleted resource", resource_id)
        else:
            raise Exception("ERROR: Uknown error check resource", resource_id, success, result)

    return success, result


def get_datastore_info(resource_ids: list, raw: False) -> list:
    datastore_info = []
    i = 1
    for resource_id in resource_ids:
        if not raw:
            print("{}/{}\t".format(i, len(resource_ids)), resource_id)

        success, result = check_resource(resource_id)
        resource_data = result['result']

        if not raw:
            print('\t', resource_data['package_id'], resource_data['name']['es'])

        success, result = ckan_api_request("package_show", "get", API_TOKEN, params={"id": resource_data['package_id']})
        package_data = result['result']

        # success, result = ckan_api_request("datastore_info", "post", API_TOKEN, data={"id": id})
        success, result = ckan_api_request("datastore_search", "post", API_TOKEN,
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


def set_column_labels(resource_ids: list, labels_dict: dict) -> int:
    # http://127.0.0.1:5000/dataset/138521831832-gva-med-cont-atmos-md-2006/dictionary/e416050a-b559-4743-b7f1-b4aaaefdc9ab

    count = 0
    for resource_id in resource_ids:
        datastore_info = get_datastore_info([resource_id], raw=True)[0]
        fields = datastore_info['fields'][1:]
        index = 1
        form_data = {}
        update = False

        for field in fields:
            form_data["info__{}__type_override".format(index)] = field.get('info',{}).get('type_override', "")
            form_data["info__{}__label".format(index)] = field.get('info', {}).get('label', "")
            form_data["info__{}__notes".format(index)] = field.get('info', {}).get('notes', "")

            base_id = unidecode(field['id'].strip().lower())
            base_id_alt = base_id.replace(' ', '_')

            if labels_dict.get(base_id, labels_dict.get(base_id_alt)):
                if not update:
                    print("\n {}/dataset/{}/resource/{} ".format(CKAN_URL, datastore_info['package_data']['name'],resource_id))
                ref_info = labels_dict.get(base_id, labels_dict.get(base_id_alt))
                print('\t - Match: {} ({}) => {} '.format(field['id'], base_id, ref_info['label']))
                form_data["info__{}__label".format(index)] = ref_info['label']
                form_data["info__{}__notes".format(index)] = ref_info['description']
                update = True
            index += 1

        if update:
            count += 1
            # set headers
            headers = {'Authorization': API_TOKEN}

            result = {}

            # do the actual call
            try:
                response = requests.post('{}/dataset/{}/dictionary/{}'.format(CKAN_URL,
                                                                datastore_info['package_data']['name'],
                                                                resource_id
                                                                ), data=form_data,headers=headers)

                # If the response was successful, no Exception will be raised
                response.raise_for_status()

            except HTTPError as http_err:
                print(f'\t HTTP error occurred: {http_err} {response.json().get("error")}')  # Python 3.6
                result = {"code": response.status_code, "http_error": http_err, "error": response.json().get("error")}
            except Exception as err:
                print(f'\t Other error occurred: {err}')  # Python 3.6
                result = {"error": err}

    return count


def main() -> int:
    labels_dict = read_labels(FILE_PATH)

    if len(sys.argv) > 1:
        resource_ids = [sys.argv[1]]
    else:
        # get_all_resources_ids_in_datastore
        success, result = get_resources_list()
        resource_ids = result

    # add labels
    matches = set_column_labels(resource_ids, labels_dict)
    print(" * DONE: {} resources updated".format(matches))
    # # gather datasets columns
    # datastore_info = get_datastore_info(resource_ids)
    #
    # with open(OUTPUT_PATH, 'w') as f:
    #     writer = csv.DictWriter(f, fieldnames=list(datastore_info[0].keys()))
    #     writer.writeheader()
    #     writer.writerows(datastore_info)
    #
    # print("DONE: Written {} rows to {}".format(len(datastore_info), OUTPUT_PATH))

    return 0


if __name__ == '__main__':
    sys.exit(main())
