from apiclient import discovery
from httplib2 import Http
from oauth2client import client, file, tools
import io
import os
import csv
import sys
import pylightxl as xl
from datetime import datetime
import json

from googleapiclient.http import MediaIoBaseDownload
from scripts.commons import commons
from unidecode import unidecode


# parameters from ENV
from dotenv import load_dotenv
load_dotenv('../.env')

MASTER_FILE_ID = os.getenv('MASTER_FILE_ID')
VALIDATION_REPORT_PATH = os.getenv('VALIDATION_REPORT_PATH')


def connect_to_google_drive(master_file_id: str) -> str:
    # define path variables
    credentials_file_path = './credentials/credentials.json'
    client_secret_file_path = './credentials/client_secret.json'

    # define API scope
    scope = 'https://www.googleapis.com/auth/drive'

    # define store
    store = file.Storage(credentials_file_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(client_secret_file_path, scope)
        credentials = tools.run_flow(flow, store)

    # define API service
    http = credentials.authorize(Http())
    drive = discovery.build('drive', 'v3', http=http)

    # pylint: disable=maybe-no-member
    request = drive.files().export_media(fileId=master_file_id,
                                         mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    file_io = io.BytesIO()
    downloader = MediaIoBaseDownload(file_io, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(F'Download {int(status.progress() * 100)}.')

    # content = file_download.getvalue()
    output_file = "./tmp/output.xlsx"
    with open(output_file, "wb") as f:
        f.write(file_io.getbuffer())
    return output_file


def export_to_csv(file_path: str):
    db = xl.readxl(file_path)
    worksheets = db.ws_names
    for ws_name in worksheets:
        worksheet = db.ws(ws_name)
        header = []
        row_dicts = []
        for row in worksheet.rows:
            if header:
                row_data = {header[i]: row[i] for i in range(len(header))}
                row_dicts += [row_data]
            else:
                header = row

        ws_output_file = './tmp/' + ws_name + '.csv'
        with open(ws_output_file, 'w') as f:
            w = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
            w.writeheader()
            for row_data in row_dicts:
                w.writerow(row_data)

        print("\t * Worksheet '{}' written to {}".format(ws_name, ws_output_file))
    return


def normalize_tag(tag: str) -> str:
    return unidecode(tag.strip().lower().replace("'", " ").replace("’", " ").replace(' ', '_')).replace('*', '_')


def validate(report_path: str) -> int:
    total_issues = 0

    with open(os.getenv('ORGANIZATION_LIST_PATH')) as jsonfile:
        organizations = [o['name'] for o in json.load(jsonfile)["organizations"]]

    with open(report_path, "w") as report:
        report.write("Validation report " + str(datetime.now()))

        # read groups
        report.write("\n* GROUPS:")
        group_list_path = './tmp/' + os.path.basename(os.getenv('GROUP_LIST_PATH'))
        groups_list = commons.read_groups(group_list_path)
        groups = {g['name']: g for g in groups_list}
        if len(groups) < len(groups_list):
            report.write("\n\t ERROR: Duplicate in groups: " + ', '.join([g['name'] for g in groups_list]) + '\n')
            total_issues += 1
            print("\t => ERRORS found in groups")
        else:
            print("\t OK")
            report.write("\t groups OK ({})".format(len(groups)))

        # read vocabulary
        vocabulary_issues = 0
        report.write("\n\n* VOCABULARY:")
        vocabulary_list_path = './tmp/' + os.path.basename(os.getenv('VOCABULARY_LIST_PATH'))
        vocabulary_list = [v['tag_vocabulary'] for v in commons.read_vocabulary(vocabulary_list_path)]
        print(" \t -> Read {} vocabulary items".format(len(vocabulary_list)))
        vocabulary = {unidecode(g['es'].strip().lower()): g for g in vocabulary_list}
        if len(vocabulary) < len(vocabulary_list):
            report.write("\n\t ERROR: Duplicate in vocabulary: " + ', '.join([g['es'] for g in vocabulary_list]))
            vocabulary_issues += 1

        for key, value in vocabulary.items():
            for lang in commons.LANGS:
                if len(value.get(lang, '').strip()) < 2:
                    report.write(
                        "\n\t ERROR: Missing translation in vocabulary: {} {}".format(lang, str(value)))
                    vocabulary_issues += 1
        total_issues += vocabulary_issues
        if vocabulary_issues == 0:
            print("\t OK")
            report.write("\t vocabulary OK ({})".format(len(vocabulary)))
        else:
            print("\t => ERRORS found in vocabulary")

        # read tags
        tags_issues = 0
        report.write("\n\n* TAGS:")
        tags_list_path = './tmp/' + os.path.basename(os.getenv('TAG_LIST_PATH'))
        tags_list = [v['tag'] for v in commons.read_vocabulary(tags_list_path)]
        tags = {normalize_tag(g['es']): g for g in tags_list}
        if len(tags) < len(tags_list):
            report.write("\n\t ERROR: Duplicate in tags: " + ', '.join([g['es'] for g in tags_list]))
            tags_issues += 1

        for key, value in tags.items():
            for lang in commons.LANGS:
                value_lang = value.get(lang, '')
                if len(value_lang.strip()) < 2:
                    report.write(
                        "\n\t ERROR: Missing translation in tags: {} {}".format(lang, str(value)))
                    tags_issues += 1
                value_fix = normalize_tag(value_lang)
                if value_fix != value_lang:
                    report.write(
                        "\n\t ERROR: Bad tag for {}: '{}' should be '{}' "
                        "(lowercase, no spaces, no accents)".format(lang, value_lang, value_fix))
                    tags_issues += 1
        total_issues += tags_issues
        if tags_issues == 0:
            print("\t OK")
            report.write("\t tags OK ({})\n".format(len(tags)))
        else:
            print("\t => ERRORS found in tags")

        # read datasets
        report.write("\n\n* DATASETS:")
        dataset_issues = 0
        datasets_list_path = './tmp/' + os.path.basename(os.getenv('DATASET_LIST_PATH'))
        print(" - Read input file: {}".format(datasets_list_path))
        datasets = {}
        with open(datasets_list_path) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

            for row in reader:
                dataset_id = row['id'].strip().lower()
                # check duplicate
                if dataset_id in datasets.keys():
                    report.write(
                        "\n\t ERROR: Duplicated dataset id: {}".format(dataset_id))
                    dataset_issues += 1
                # check
                dataset = {k: v for k, v in row.items() if len(k.strip()) > 0}
                datasets[dataset_id] = dataset

                if len(dataset['ok']) == 0 or round(float(dataset['ok'])) != 0:
                    if dataset['organization'] not in organizations:
                        report.write(
                            "\n\t ERROR: Dataset '{}', bad organization '{}'".format(dataset_id, dataset['organization']))
                        dataset_issues += 1
                    # groups,groups_extra
                    dataset_groups = [g.strip() for g in [dataset['groups']] + dataset['groups_extra'].split(',')
                                      if len(g.strip()) > 0]
                    for group in dataset_groups:
                        if group not in groups.keys():
                            report.write(
                                "\n\t ERROR: Dataset '{}', bad group '{}'".format(dataset_id, group))
                            dataset_issues += 1

                    # vocabulary,vocabulary_extra
                    dataset_vocabulary = [unidecode(g.strip().lower()) for g in [dataset['vocabulary']]
                                          + dataset['vocabulary_extra'].split(',') if len(g.strip()) > 0]
                    for item in dataset_vocabulary:
                        if item not in vocabulary.keys():
                            report.write(
                                "\n\t ERROR: Dataset '{}', bad vocabulary '{}'".format(dataset_id, item))
                            dataset_issues += 1

                    # tags
                    dataset_tags = [g.strip() for g in dataset['tags'].split(',') if len(g.strip()) > 0]
                    for item in dataset_tags:
                        if item not in tags.keys():
                            if normalize_tag(item) in tags.keys():
                                report.write(
                                    "\n\t ERROR: Dataset '{}', bad tag '{}' (rewrite to '{}')"
                                    .format(dataset_id, item, normalize_tag(item)))
                            else:
                                report.write(
                                    "\n\t ERROR: Dataset '{}', bad tag '{}' (missing)".format(dataset_id, item))
                            dataset_issues += 1

        if dataset_issues == 0:
            print("\t OK")
            report.write("\t datasets OK ({})\n".format(len(datasets)))
        else:
            print("\t => ERRORS found in datasets")

        total_issues += dataset_issues

    return total_issues


if __name__ == '__main__':
    if len(sys.argv) > 1:
        # skip google download
        output_file = sys.argv[1]
    else:
        output_file = connect_to_google_drive(MASTER_FILE_ID)
        # output_file = "./tmp/output.xlsx"
    export_to_csv(output_file)
    issues = validate(VALIDATION_REPORT_PATH)
    print(" DONE, issues = {}".format(issues))
