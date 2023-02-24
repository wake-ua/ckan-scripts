import json
from django.contrib.gis.geos import Polygon

ORG_FILE = "./data/organization_list.json"
GEOJSON_FILE = "./data/ca_municipios_20230105.geojson"


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


# read the datasets files
print(" * Read ", ORG_FILE)

with open(ORG_FILE) as jsonfile:
    organizations = json.load(jsonfile)

print("\t - Got {} organizations: {} ".format(len(organizations["organizations"]), ', '.join([o["name"] for o in organizations["organizations"]])))

code_selection = []
for organization in organizations["organizations"]:
    print(organization)
    code_muniine = organization.get("muniine")
    if code_muniine:
        num_muniine = int(code_muniine)
        code_selection += [num_muniine]
print(code_selection)

with open(GEOJSON_FILE) as geojsonfile:
    features = json.load(geojsonfile)["features"]

for feature in features:
    feature_code = int(feature["properties"]["MUNIINE"])
    if feature_code in code_selection:
        geometry = feature["geometry"]
        if geometry["type"] == "Polygon":
            spatial = transform_location(geometry)
            print('\n', feature_code, feature["properties"]["NOMBRE"], '\n', json.dumps(spatial))
        else:
            raise ("Bad geometry type: " + geometry["type"])
