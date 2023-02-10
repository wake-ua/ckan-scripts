#!/usr/bin/env python
import requests
from requests.exceptions import HTTPError
import json
import pprint
import csv
import io
import shutil
import os
import sys
import datetime
from django.contrib.gis.geos import Polygon

# parameters from ENV
from dotenv import load_dotenv

load_dotenv('../.env')

API_TOKEN = os.getenv('API_TOKEN')
CKAN_URL = os.getenv('CKAN_URL')

# parameters
FILE_DIR = "./data"
TMP_DIR = os.path.join(FILE_DIR, 'tmp')
PREVIEW_LINES = 10

CKAN_API_URL = "{}/api/3/action/".format(CKAN_URL)

default_locations = {"ajuntament-alcoi": {"type": "Polygon", "coordinates": [
    [[715092.573709193151444, 4290316.587757777422667], [715170.72468690527603, 4290343.026466369628906],
     [715270.723552905838005, 4290623.96133420150727], [715912.748782854061574, 4290823.959395665675402],
     [716218.761776866740547, 4291248.018417477607727], [716663.757870981353335, 4291208.018719019368291],
     [717818.746982860495336, 4290724.897748008370399], [717811.715675718849525, 4290234.902394608594477],
     [719567.714958137599751, 4289105.850134472362697], [719626.698830080451444, 4288895.85167841706425],
     [720253.724041916429996, 4288538.980120005086064], [720417.706875170115381, 4288280.8571637282148],
     [720949.655140671413392, 4288348.044171045534313], [721669.648473130888306, 4288109.921196753159165],
     [721933.708361363038421, 4287223.991667378693819], [721810.663041123189032, 4286893.99486168846488],
     [722280.658306433702819, 4286762.121187319979072], [722980.651871335343458, 4285284.009668275713921],
     [722972.177640339010395, 4284847.850895561277866], [722947.683194867451675, 4283587.150343818590045],
     [723574.629950621747412, 4282562.159780924208462], [723438.694041280657984, 4281704.042936975136399],
     [723795.64369747065939, 4280849.988186108879745], [723814.627243071212433, 4279517.187713179737329],
     [723088.696643540635705, 4278579.071709525771439], [722440.655489506316371, 4278365.01119989156723],
     [721927.691555315512232, 4278580.946816941723228], [721070.668441049638204, 4279504.063655543141067],
     [721099.652553774532862, 4279894.997122879140079], [720674.656580170732923, 4280159.057209450751543],
     [720442.705802905606106, 4280018.12133218254894], [720438.721594000817277, 4280922.175436372868717],
     [720290.676135121728294, 4281074.986589977517724], [719900.679532785201445, 4280602.178476817905903],
     [719050.687429318204522, 4280114.99565287027508], [718971.703256563981995, 4278784.070630417205393],
     [718035.696365443058312, 4277614.081206882372499], [715433.767680071759969, 4281755.918477905914187],
     [715414.03119776409585, 4281756.802664711140096], [713731.752019644598477, 4281832.168120340444148],
     [712406.764491775305942, 4281384.984973153099418], [712064.73631928418763, 4281649.982506167143583],
     [710098.816766911186278, 4281612.170372801832855], [709750.773046669200994, 4280984.051546212285757],
     [708920.780543712317012, 4280792.178349299356341], [708573.830538862966932, 4280904.989893683232367],
     [708263.833850725903176, 4281488.109537940472364], [707654.777053611702286, 4281419.047593209892511],
     [706815.800192240159959, 4282372.163757322356105], [706184.790543327224441, 4282064.979467236436903],
     [705853.855741970706731, 4282123.103769154287875], [705807.840758358943276, 4282812.159814447164536],
     [705777.840942375711165, 4283117.156939191743731], [706942.830535738612525, 4284174.959520334377885],
     [707183.84418929903768, 4284699.954839564859867], [707270.796386934467591, 4285009.014633876271546],
     [707954.774547579116188, 4285583.071334433741868], [709255.778209006763063, 4286224.003242642618716],
     [710067.802354171290062, 4287208.993989516049623], [712905.744768728385679, 4288233.984090357087553],
     [714274.717010502703488, 4290039.904398830607533], [715092.573709193151444, 4290316.587757777422667]]]}
    }


def transform_location(geometry: dict) -> dict:
    new_location = {"type": geometry["type"], "coordinates": []}
    # TODO: check for other geometry, this was only tested for Polygons
    data = geometry["coordinates"][0]
    poly_municipio = Polygon(data, srid=32630)
    poly_gps = poly_municipio.transform(4326, clone=True)
    coordinates = []
    for c1, c2 in poly_gps.coords[0]:
        coordinates = [[c1, c2]] + coordinates
    new_location["coordinates"] = [coordinates]
    return new_location


def read_datasets(file_dir: str) -> list:
    # read the datasets files
    print(" - Read input dir: {}".format(file_dir))

    datasets = []

    for file in os.listdir(file_dir):
        if file.endswith(".json"):
            file_path = os.path.join(file_dir, file)
            print("\t * Read ", file_path)

            with open(file_path) as jsonfile:
                dataset = json.load(jsonfile)["result"]
                datasets += [dataset]

                print("\t\t => Got '{}' ({})".format(dataset["name"], dataset["organization"]["name"]))

    print(" \t => Read {} dataset(s): {}".format(len(datasets),
                                                 ', '.join([dataset['name'] for dataset in datasets])))

    return datasets


def ckan_api_request(endpoint: str, method: str, token: str, data: dict = {},
                     params: dict = {}, files: list = [], dump: bool = True,
                     content: str = 'application/json') -> (int, dict):
    # set headers
    headers = {'Authorization': token}
    if content:
        headers['Content-Type'] = content

    # do the actual call
    try:
        if method == 'post':
            if dump:
                data = json.dumps(data)
            response = requests.post('{}{}'.format(CKAN_API_URL, endpoint), data=data, params=params,
                                     files=files, headers=headers)
        else:
            response = requests.get('{}{}'.format(CKAN_API_URL, endpoint), params=params, headers=headers)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        result = response.json()
        return 0, result

    except HTTPError as http_err:
        print(f'\t HTTP error occurred: {http_err} {response.json()}')  # Python 3.6
        result = {"http_error": http_err, "error": response.json()}
    except Exception as err:
        print(f'\t Other error occurred: {err}')  # Python 3.6
        result = {"error": err}

    return -1, result


# def handle_csv_resource(ckan_resource: dict) -> (str, dict):
#     file_name = ckan_resource["url"].split("/")[-1]
#     response = requests.get(ckan_resource["url"])
#     buff = io.StringIO(response.text)
#     cr = csv.DictReader(buff)
#     selected_lines = []
#     for row in cr:
#         selected_lines += [row]
#         if len(selected_lines) >= PREVIEW_LINES:
#             break
#
#     if selected_lines:
#         file_name = os.path.join(TMP_DIR, 'PREVIEW_' + ckan_resource["id"]
#         + '_' + ckan_resource["url"].split("/")[-1])
#         header = [k for k in selected_lines[0].keys()]
#
#         print("\t  *  Saving preview at: " + file_name)
#         with open(file_name, 'w', newline='') as csvfile:
#             writer = csv.DictWriter(csvfile, fieldnames=header)
#
#             writer.writeheader()
#             for line in selected_lines:
#                 writer.writerow(line)
#
#         ckan_resource["description"] += "\n\n Data header: " + ', '.join(header)
#         ckan_resource["description"] += "\n\n Full data available at: " + ckan_resource["url"]
#
#     return file_name, ckan_resource


def edit_dataset(dataset: dict, update: bool = False) -> (int, dict):
    # map attributes to ckan dataset
    ckan_dataset = {"name": dataset["name"], "title": dataset["title"]["es"], "notes": dataset["notes"]["es"],
                    "owner_org": dataset["organization"]["name"], "license_id": dataset["license_id"],
                    "spatial": json.dumps(transform_location(default_locations[dataset["organization"]["name"]]))}

    # spatial (Check for multi-polygon)

    # check resources
    ckan_resources = []
    for resource in dataset["resources"]:
        ckan_resource = {}
        ckan_resource["id"] = resource["id"]
        ckan_resource["url"] = resource["url"]
        ckan_resource["name"] = resource["name_es"]
        ckan_resource["description"] = resource.get("description_es", resource["description"])
        ckan_resource["format"] = resource["format"]
        ckan_resource["size"] = resource["size"]
        ckan_resource["mimetype"] = resource["mimetype"]
        ckan_resources += [ckan_resource]

    if ckan_resources:
        ckan_dataset["resources"] = ckan_resources

    # call the endpoint
    if not update:
        success, result = ckan_api_request(endpoint="package_create", method="post", token=API_TOKEN,
                                           data=ckan_dataset)
    else:
        ckan_dataset["id"] = dataset["name"]
        success, result = ckan_api_request(endpoint="package_patch", method="post",
                                           token=API_TOKEN, data=ckan_dataset)
    return success, result


def update_resource(resource: dict) -> (int, dict):
    success = -1
    result = {}

    if resource["url"].endswith(".csv"):
        file_name, ckan_resource = handle_csv_resource(resource)
        if os.path.isfile(file_name):
            # call the update resource endpoint
            keys = ["description", "format", "id", "mimetype", "name", "package_id", "size"]
            data = {k: ckan_resource[k] for k in keys}
            success, result = ckan_api_request(endpoint="resource_create", method="post",
                                               token=API_TOKEN, data=data,
                                               files=[('upload', open(file_name, 'rb'))], dump=False,
                                               content="")
            # content="multipart/form-data")
            os.remove(file_name)

    return success, result


def main() -> int:
    created_datasets = []
    updated_datasets = []

    # read the input file
    datasets = read_datasets(FILE_DIR)

    # save the organizations
    for dataset in datasets:
        print("\n * Creating DATA: {}".format(dataset["name"]))
        success, result = edit_dataset(dataset)
        if success >= 0:
            print("\t * Created: {}...".format(str(result)[:500]))
            created_datasets += [dataset["name"]]
        else:
            print("\t => Created Failed, trying UPDATE...")
            success, result = edit_dataset(dataset, update=True)
            if success >= 0:
                print("\t * Updated: {}...".format(str(result)[:500]))
                updated_datasets += [dataset["name"]]
            else:
                print("\t => * Update Failed *")
                return -1

        # updated_dataset = result["result"]
        #
        # # add previews to csv resources
        # for resource in updated_dataset["resources"]:
        #     if resource["url"].endswith(".csv"):
        #         print("\t * UPDATING csv resource {} (preview upload)...".format(resource['name']))
        #         success, result = update_resource(resource)
        #         if success >= 0:
        #             print("\t * Updated csv resource : {}...".format(str(result)[:500]))
        #         else:
        #             print("\t => ERROR: * Resource csv pdate Failed *")
        #             return -1

    print(" \t - Created {} datasets: {} "
          "\n\t - Updated {} datasets: {}".format(len(created_datasets), ', '.join(created_datasets),
                                                  len(updated_datasets), ', '.join(updated_datasets)))

    success, total_datasets = ckan_api_request(endpoint="package_list", method="get", token=API_TOKEN)
    if success >= 0:
        print("\n - CKAN Datasets ({}): {}".format(len(total_datasets["result"]), ', '.join(total_datasets["result"])))

    else:
        print("\t => * ERROR: Retrieving All Datasets Failed *")
        return -1

    return 0


if __name__ == '__main__':
    sys.exit(main())
