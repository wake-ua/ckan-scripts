from apiclient import discovery
from httplib2 import Http
from oauth2client import client, file, tools
import io
import csv
import pylightxl as xl

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


# define a function to retrieve all files
def retrieve_all_files(api_service):
    results = []
    page_token = None

    while True:
        try:
            param = {}

            if page_token:
                param['pageToken'] = page_token

            files = api_service.files().list(**param).execute()            # append the files from the current result page to our list
            results.extend(files.get('files'))            # Google Drive API shows our files in multiple pages when the number of files exceed 100
            page_token = files.get('nextPageToken')

            if not page_token:
                break

        except errors.HttpError as error:
            print(f'An error has occurred: {error}')
            break    # output the file metadata to console
    for drive_file in results:
        if drive_file.get('mimeType') == 'application/vnd.google-apps.spreadsheet':
            print(drive_file)

    return results


def connect_to_googledrive():
    # define path variables
    credentials_file_path = './credentials/credentials.json'
    clientsecret_file_path = './credentials/client_secret.json'

    # define API scope
    SCOPE = 'https://www.googleapis.com/auth/drive'

    # define store
    store = file.Storage(credentials_file_path)
    credentials = store.get() # get access token
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(clientsecret_file_path, SCOPE)
        credentials = tools.run_flow(flow, store)

    # define API service
    http = credentials.authorize(Http())
    drive = discovery.build('drive', 'v3', http=http)

    # call the function
    # all_files = retrieve_all_files(drive)

    # master_file_id = [f for f in all_files if  f['name'] == 'TDATA_UA_MASTER'][0]['id']
    master_file_id = '1_hoie15DfKl6P5iLTHqF_Fm5MhvT5P0HkUXt29iSQA4'
    print('Found: ' + master_file_id)

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


def export_to_csv(output_file):
    db = xl.readxl(output_file)
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
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            for row_data in row_dicts:
                w.writerow(row_data)

        print("\t * Worksheet '{}' written to {}".format(ws_name, ws_output_file))


if __name__ == '__main__':
    output_file = connect_to_googledrive()
    output_file = "./tmp/output.xlsx"
    export_to_csv(output_file)