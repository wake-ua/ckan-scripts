#!/usr/bin/env python
import json
import os


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

    # special for torrent
    if org_name == "torrent":
        multi_value = new_field["es"].split("/ ")
        if len(multi_value) == 2:
            new_field["ca"] = multi_value[0].strip()
            new_field["es"] = multi_value[1].strip()
            new_field["en"] = multi_value[1].strip()

    # special for sagunto
    if org_name == "sagunto":
        multi_value = new_field["es"].split(" / ")
        if len(multi_value) == 2:
            new_field["es"] = multi_value[0]
            new_field["ca"] = multi_value[1]
            new_field["en"] = multi_value[0]
        else:
            multi_value = new_field["es"].split(" - ")
            if len(multi_value) == 2:
                new_field["es"] = multi_value[0]
                new_field["ca"] = multi_value[1]
                new_field["en"] = multi_value[0]

    # special for aoc
    if org_name == "aoc":
        value = dataset.get(field + '_translated', {})
        if value and isinstance(value, dict):
            new_field["ca"] = value.get("ca", new_field["ca"])
            new_field["es"] = value.get("es", new_field["es"])
            new_field["en"] = value.get("en", new_field["en"])

    if org_name == "gva":
        separator = "\r\n\r\n----------------------------------------------------------------------------------\r\n\r\n"
        value = dataset.get(field, "")
        if value.find(separator) > 0:
            new_field["ca"] = value.split(separator)[0]
            new_field["es"] = value.split(separator)[1]
            new_field["en"] = new_field["es"]

    if mandatory:
        langs = list(new_field.keys())
        some_value = dataset["name"]
        for lang in langs:
            if len(new_field[lang])>0:
                some_value = new_field[lang]
                break
        for lang in langs:
            if len(new_field[lang]) == 0:
                new_field[lang] = some_value

    return new_field


def complete_ckan_dataset(path: str, organization: dict, ckan_dataset: dict) -> (int, dict):

    # Get full CKAN metadata
    full_dataset = read_dataset(os.path.join(os.path.dirname(path), 'all' + os.path.basename(path)[4:]))

    # map attributes to ckan dataset
    ckan_dataset["title"] = get_translated_field("title", full_dataset, full_dataset["name"], organization["name"],
                                                 mandatory=True)
    ckan_dataset["notes"] = get_translated_field("notes", full_dataset, full_dataset["title"], organization["name"])

    # check resources
    full_resources = {r["url"]: r for r in full_dataset["resources"]}
    ckan_resources = []
    for ckan_resource in ckan_dataset["resources"]:
        full_resource = full_resources[ckan_resource["url"]]

        ckan_resource["name"] = get_translated_field("name", full_resource, full_resource["id"], organization["name"])
        ckan_resource["description"] = get_translated_field("description", full_resource, full_resource["name"],
                                                            organization["name"])
        for field in ["format", "mimetype", "size"]:
            ckan_resource[field] = full_resource[field]

        # unify resource formats
        if ckan_resource["format"].strip().lower() == "shape":
            ckan_resource["format"] = "Shape"

        ckan_resources += [ckan_resource]

    if ckan_resources:
        ckan_dataset["resources"] = ckan_resources

    return ckan_dataset
