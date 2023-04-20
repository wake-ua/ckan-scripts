#!/usr/bin/env python
import os
import sys
import subprocess
from scripts.commons import commons

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

CKAN_URL = os.getenv('CKAN_URL')
CKAN_CONFIG = os.getenv('CKAN_CONFIG')

# parameters
CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)
OUTPUT_PATH = "./output/datastore_checkup.txt"


def get_datastore_resources_list() -> (int, list):
    success, result = commons.ckan_api_request("wakeua_list_datastore_resources", "get")
    if success >= 0:
        return success, result['result']
    return success, result


def get_csv_resources_list() -> (int, list):
    step = 500
    params = {"q": "res_format:CSV", "rows": step}
    success, result = commons.ckan_api_request("package_search", "get", params=params)
    if success < 0:
        raise ("ERROR: Cannot retrieve datasets")
    total = result['result']['count']
    datasets = result['result']['results']

    while len(datasets) < total and len(result['result']['results']) > 0:
        params = {"q": "res_format:CSV", "rows": step, "start": len(datasets)}
        success, result = commons.ckan_api_request("package_search", "get", params=params)
        if success < 0:
            raise ("ERROR: Cannot retrieve datasets")
        datasets += result['result']['results']

    resources = []

    for dataset in datasets:
        resources += [r for r in dataset['resources'] if r['format'] == 'CSV' and not r['datastore_active']]
    return resources


def delete_datastore_resource(resource_id: str) -> (int, dict):
    print("\t * DELETE *", resource_id)
    success, result = commons.ckan_api_request("datastore_delete", "post",
                                               data={"resource_id": resource_id, 'force': True})
    if success >= 0:
        return success, result['result']
    return success, result


def check_resources(resource_ids: list) -> (int, list):
    for resource_id in resource_ids:
        success, result = commons.ckan_api_request("resource_show", "get", params={"id": resource_id})
        if success < 0:
            if result["code"] == 404:
                print("ERROR: NOT FOUND \t" + resource_id)
                print(result["http_error"].errno)
                success, result = delete_datastore_resource(resource_id)
                print(" => DELETED resource: " + resource_id, success, result)
                # raise Exception("ERROR: Found deleted resource", resource_id)
                continue
            else:
                raise Exception("ERROR: Unknown error check resource", resource_id, success, result)

        if result['result']['format'].upper() != 'CSV':
            print('* Bad format:', result['result']['format'], resource_id)
            success, result = delete_datastore_resource(resource_id)
            print(" => DELETED resource: " + resource_id, success, result)
            # raise Exception("ERROR: Found bad format resource", resource_id)
        if not result['result']['url'].lower().find('csv') > 0 and not result['result']['url'].find('txt') > 0:
            print('* MAYBE Bad format:', result['result']['format'], resource_id, result['result']['url'])
    return


def reload_resources(resources, force=False):
    with open(OUTPUT_PATH, 'w') as error_log:
        datasets = {r['package_id']: [] for r in resources}
        for r in resources:
            datasets[r['package_id']] += [r]

        print(" - Missing resources result in {} datasets".format(len(datasets.keys())))
        i = 1
        for dataset_id in datasets.keys():
            print("\n * Dataset {}/{}".format(i, len(datasets.keys())))
            resources_to_upload = 0
            if not force:
                for r in datasets[dataset_id]:
                    resource_id = r["id"]
                    # check errors
                    success, result = commons.ckan_api_request("xloader_status", "get", params={"id": resource_id})
                    if success >= 0:
                        if result['result']['status'] == "error":
                            error_message = result['result']['error']
                            if result['result']['task_info']:
                                if result['result']['task_info']['error']:
                                    error_message = result['result']['task_info']['error']['message']
                            print("\t - Xloader ERROR:", resource_id, error_message,
                                  "{}/dataset/{}/resource/{}\n".format(CKAN_URL, dataset_id, resource_id))
                            error_log.write("\n * {} {}/dataset/{}/resource/{}\n".format(i, CKAN_URL, dataset_id,
                                                                                         resource_id))
                            error_log.write("{}{}\n".format(resource_id, error_message))
                            # retry on network error
                            if error_message.find("HTTPSConnectionPool") >= 0 \
                                    or error_message.find("psycopg2") >= 0 \
                                    or error_message.find("status=500") >= 0 \
                                    or error_message.find("HTTPConnection") >= 0:
                                resources_to_upload += 1
                        else:
                            resources_to_upload += 1
                    else:
                        resources_to_upload += 1

            if resources_to_upload > 0 or force:
                command = "ckan -c {} xloader submit {}".format(CKAN_CONFIG, dataset_id)
                print("\t Submitting dataset {}/{}\n\t".format(i, len(datasets.keys())), command)
                p = subprocess.run(command.split(' '))
                print("DONE: {}/dataset/{}/resource/{}\n".format(CKAN_URL,dataset_id,
                                                        [r["id"] for r in resources if r['package_id'] == dataset_id][0]))
                p1 = subprocess.run(['sleep', "5"])

            i += 1


def main() -> int:

    # get_all_resources_ids_in_datastore
    success, result = get_datastore_resources_list()
    resource_ids = result
    print("Found {} resources in datastore".format(len(resource_ids)))

    # delete removed resources
    check_resources(resource_ids)

    # check all CSV resources are in the datastore
    csv_resources_missing = get_csv_resources_list()
    print("Found {} resources missing in datastore: {}...".format(len(csv_resources_missing),
                                                    [(r['id'], r['package_id']) for r in csv_resources_missing[0:10]]))
    missing_matches = [(r['id'], r['package_id']) for r in csv_resources_missing if r['id'] in resource_ids]
    print(len(missing_matches), missing_matches[0:10])

    dataset_list = list(set([r['package_id'] for r in csv_resources_missing]))
    print(" - Missing resources result in {} datasets".format(len(dataset_list)))

    reload_resources(csv_resources_missing)
    return 0


if __name__ == '__main__':
    sys.exit(main())
