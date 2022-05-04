from io import BytesIO
from logging import info
from os import environ as env
from typing import Dict, List, Any

import pandas as pd
from apiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from yaml import safe_load


class GoogleDriveClient:
    def __init__(self, gapi_keyfile=None):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        keyfile = safe_load(gapi_keyfile or env["GCP_SERVICE_CREDS"])
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile, scope)
        self.service = build(
            "drive", "v3", credentials=credentials, cache_discovery=False
        )

    def get_data_frame_from_file_id(self, file_id: str) -> pd.DataFrame:
        """
        Google drive does not allow direct csv reading from the urls, so we need to
        download the file using their API method, create a df and then delete the local file

        :return: pandas Dataframe of data available in file_id
        """
        request = self.service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

        bytes_data = fh.getvalue()
        df = pd.read_csv(BytesIO(bytes_data))
        return df

    def get_item_id(
        self, item_name: str, in_folder_id: str = None, is_folder: bool = None
    ) -> str:
        """
        Retrieves the unique identifier for a folder or file available in Google Drive.
        The folder / file must have been shared with whatever account is running this script

        :param self:
        :param item_name: Item name to be retrieve
        :param in_folder_id: Optional, specify a folder to look in
        :param is_folder: Optional, specify if this is a folder.
        :return: str of unique identifier in Google Drive
        """

        query = f"fullText contains '{item_name}'"

        if is_folder:
            query = f"{query} and mimeType='application/vnd.google-apps.folder' "

        if in_folder_id:
            query = f"{query} and '{in_folder_id}' in parents"

        # Call the Drive v3 API
        results = (
            self.service.files()
            .list(q=query, pageSize=10, fields="nextPageToken, files(id)")
            .execute()
        )
        items = results.get("files", [])
        if not items:
            return ""
        else:
            return items[0].get("id")

    def get_archive_folder_id(self, in_folder_id) -> str:
        """

        convenience function which creates an archive folder if it doesn't already exist.
        created to reduce complexity in main function,

        :param self:
        :param in_folder_id: folder in which to look
        :return: folder_id of archive folder
        """
        archive_folder_id = self.get_item_id("Archive", in_folder_id, True)

        if archive_folder_id is None:
            archive_folder_id = self.create_folder("Archive", in_folder_id)

        return archive_folder_id

    def create_folder(self, folder_name: str, in_folder_id: str) -> str:
        """

        :param folder_name:
        :param in_folder_id:
        :return: folder_id of folder which was created.
        """
        file_metadata: Dict[str, Any] = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
        }

        if in_folder_id:
            file_metadata.update({"parents": [in_folder_id]})

        created_folder = (
            self.service.files().create(body=file_metadata, fields="id").execute()
        )
        info(f"Folder {folder_name} created successfully")

        folder_id = created_folder.get("id")

        return folder_id

    def get_files_in_folder(self, folder_id: str, file_type: str) -> List[Dict]:
        """
            Retrieves a list of all files of a specific type available in a specific folder

        :param folder_id: Folder to retrieve
        :param file_type: File types to retrieve
        :return: A list of dicts containing file details. Fields in dict are controlled by the call
        to service.files
        """
        query = (
            f"'{folder_id}' in parents "
            f"and mimeType != 'application/vnd.google-apps.folder'"
        )

        if file_type:
            query = f"{query} and mimeType='{file_type}'"

        page_token = None
        all_results: List[Dict] = []

        while True:
            if page_token:

                results = (
                    self.service.files()
                    .list(
                        pageToken=page_token,
                        q=query,
                        pageSize=10,
                        fields="nextPageToken, files(id, name, mimeType)",
                    )
                    .execute()
                )

            else:

                results = (
                    self.service.files()
                    .list(
                        q=query,
                        pageSize=10,
                        fields="nextPageToken, files(id, name, mimeType)",
                    )
                    .execute()
                )

                items: List[Dict] = results.get("files", [])

                if items:
                    all_results = all_results[:] + items[:]

            page_token = results.get("nextPageToken")
            if not page_token:
                break

        return all_results

    def move_file_to_folder(self, file_id: str, to_folder_id: str) -> bool:
        """

        :param self:
        :param file_id: file to be moved
        :param to_folder_id: folder id to move to
        :return:
        """
        # Retrieve the existing parents to remove
        file = self.service.files().get(fileId=file_id, fields="parents").execute()

        previous_parents = ",".join(file.get("parents"))

        # Move the file to the new folder
        self.service.files().update(
            fileId=file_id,
            addParents=to_folder_id,
            removeParents=previous_parents,
            fields="id, parents",
        ).execute()

        info(f"{file_id} moved to {to_folder_id}")

        return True
