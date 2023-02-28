import json
import sys
from django.contrib.gis.geos import Polygon

ORG_FILE = "./data/organization_list.json"
GEOJSON_FILE = "./data/ca_municipios_20230105.geojson"


def transform_location(geometry: dict) -> dict:
    new_location = {"type": geometry["type"], "coordinates": []}
    # TODO: check for other geometry, this was only tested for Polygons

    if geometry["type"] == "Polygon":
        data = geometry["coordinates"][0]
        poly_municipio = Polygon(data, srid=32630)
        poly_gps = poly_municipio.transform(4326, clone=True)
        coordinates = []
        for c1, c2 in poly_gps.coords[0]:
            coordinates = [[c1, c2]] + coordinates
        new_location["coordinates"] = [coordinates]

    elif geometry["type"] == "MultiPolygon":
        new_coordinates = []
        for coordinates in geometry["coordinates"]:
            data = coordinates[0]
            poly_municipio = Polygon(data, srid=32630)
            poly_gps = poly_municipio.transform(4326, clone=True)
            coordinates = []
            for c1, c2 in poly_gps.coords[0]:
                coordinates = [[c1, c2]] + coordinates
            new_coordinates += [[coordinates]]
        new_location["coordinates"] = new_coordinates
    return new_location


def main():
    # read the datasets files
    print(" * Read ", ORG_FILE)

    with open(ORG_FILE) as jsonfile:
        organizations = json.load(jsonfile)

    print("\t - Got {} organizations: {} ".format(len(organizations["organizations"]), ', '.join([o["name"] for o in organizations["organizations"]])))

    # MUNICIPIOS
    code_municipio = []
    code_provincia = []
    for organization in organizations["organizations"]:
        print(organization)
        code_muniine = organization.get("muniine")
        if code_muniine:
            num_muniine = int(code_muniine)
            code_municipio += [num_muniine]
        else:
            cod_provincia = organization.get("cod_provincia")
            if cod_provincia:
                cod_provincia = int(cod_provincia)
                code_provincia += [cod_provincia]
    print(code_municipio, code_provincia)

    with open(GEOJSON_FILE) as geojsonfile:
        features = json.load(geojsonfile)["features"]

    for feature in features:
        muni_code = int(feature["properties"].get("MUNIINE", "-1"))
        prov_code = int(feature["properties"].get("cod_provincia", "-1"))
        if muni_code in code_municipio or prov_code in code_provincia:
            name = feature["properties"].get("NOMBRE", feature["properties"].get("Texto", "undefined"))
            print('\n', prov_code, prov_code, name)
            geometry = feature["geometry"]
            if geometry["type"] in ["Polygon", "MultiPolygon"]:
                spatial = transform_location(geometry)
                print(prov_code, prov_code, name, '\n', json.dumps(spatial))
            else:
                raise Exception("Bad geometry type: " + geometry["type"])

    # Provincias


if __name__ == '__main__':
    sys.exit(main())