# first time when we run the scraper this part of code arrange the files according new on top
from itemadapter import ItemAdapter
import os
import time
import csv
import requests
from zipfile import ZipFile
import zipfile
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


class NasdaqPipeline:
    def __init__(self):
        self.csv_file = 'output.csv'
        self.headers_written = self.check_headers_written()
        self.credentials = service_account.Credentials.from_service_account_file('my_credentials.json')
        self.drive_service = build('drive', 'v2', credentials=self.credentials)

    def check_headers_written(self):
        return os.path.exists(self.csv_file)

    def write_headers(self):
        with open(self.csv_file, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['id', 'displyname', 'date', 'pagecount', 'consultant', 'authortype', 'authorDescription', 'stretergy', 'notes']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

    def upload_to_drive(self,item):
        u_file_name = f"{item['id']}_{item['displyname']}.zip"
        # my id :  1Q29IIzqCFFWXR1uqetdtXNqJjX4ApqFn      jarrypatel2278@gmail.com
        # lisa id : 1T8Dj_m3NL-2NmeHt8TY3nFlo8DdJ2dMD     lisa@virtualpta.com
        file_metadata = {
                    'name': u_file_name,
                    'title': u_file_name,
                    'parents': [{'id': '1T8Dj_m3NL-2NmeHt8TY3nFlo8DdJ2dMD'}],
                    'mimeType':'application/zip'
                }
        media = MediaFileUpload(f'downloaded_files/{u_file_name}', mimetype='application/zip')
        file = self.drive_service.files().insert(body=file_metadata, media_body=media, fields='id').execute()
        # time.sleep(5)
        new_permission = {
            'value': 'lisa@virtualpta.com',
            'type': 'user',
            'role': 'reader'
            }
        permission = self.drive_service.permissions().insert(fileId=file['id'], body=new_permission).execute()
        print('File ID:', file.get('id'))

    def download_pdf(self,item,headers):
        folder_path = "downloaded_files"
        os.makedirs(folder_path, exist_ok=True)

        download_url = f"https://app.evestment.com/api/ppiq/v1/documents/{item['id']}/file?linksource=1"
        d_response = requests.get(download_url, headers=headers, verify=False)
        if d_response.status_code == 200:
            # Extract the filename from the URL
            filename = f"{item['id']}_{item['displyname']}.pdf"
            file_path = os.path.join(folder_path, filename)   
            # Save the file
            with open(file_path, 'wb') as file:
                file.write(d_response.content)
            self.zip_pdf(filename)    
            print(f"File {filename} saved successfully.")
        else:
            print(f"Failed to download file from {download_url}. Status code: {d_response.status_code}")

    def delete_files(self,item):
        # remove the file after upload
        folder_path = "downloaded_files"
        # remove pdf files
        filename = f"{item['id']}_{item['displyname']}.pdf"
        file_path = os.path.join(folder_path, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"{file_path} has been deleted successfully.")
        else:
            print(f"The file {file_path} does not exist.")

        # remove zip files
        zip_filename = f"{item['id']}_{item['displyname']}.zip"
        zipfile_path = os.path.join(folder_path, zip_filename)
        if os.path.exists(zipfile_path):
            os.remove(zipfile_path)
            print(f"{zipfile_path} has been deleted successfully.")
        else:
            print(f"The file {zipfile_path} does not exist.")

    def zip_pdf(self,filename):
        filename2 = filename.split('.')[0]
        with ZipFile(f'downloaded_files/{filename2}.zip', 'w',compression=zipfile.ZIP_DEFLATED, 
            compresslevel=9) as zipf:
            zipf.write(f'downloaded_files/{filename}',arcname=filename)

# this code is if we run the code first time and there is no any output.csv file
    # def process_item(self,item,spider):
    #     if not self.headers_written:
    #         self.write_headers()
    #         self.headers_written = True
    #     headers = item.pop("headers")
    #     with open(self.csv_file, 'a', newline='', encoding='utf-8') as csvfile:
    #         fieldnames = item.keys()
    #         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #         # Write item data
    #         writer.writerow(item)

    #     self.download_pdf(item,headers)
    #     self.upload_to_drive(item)
    #     self.delete_files(item)

    #     return item


# after this when we run the scraper this code arrange the new file on top

    def process_item(self, item, spider):
        
        if not self.headers_written:
            self.write_headers()
            self.headers_written = True
        headers = item.pop('headers')

        with open(self.csv_file, 'r', newline='', encoding='utf-8') as csvfile:
            existing_data = list(csv.DictReader(csvfile))
        # Get item data
        item_data = ItemAdapter(item).asdict()
        # Concatenate new item data on top of existing data
        merged_data = [item_data] + existing_data

        # Rewrite the CSV file with the merged data
        with open(self.csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = merged_data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(merged_data)

        self.download_pdf(item,headers)
        self.upload_to_drive(item)
        self.delete_files(item)

        return item
            
            

    def list_folder(self,parent_folder_id=None, delete=False):
        """List folders and files in Google Drive."""
        all_files = []
        page_token = None
        while True:
            results = (
                self.drive_service.files()
                .list(
                    q=(
                        f"'{parent_folder_id}' in parents and trashed=false"
                        if parent_folder_id
                        else None
                    ),
                    pageSize=1000,
                    fields="nextPageToken, files(id, name, mimeType)",
                    pageToken=page_token,
                )
                .execute()
            )

            items = results.get("files", [])
            if not items:
                print("No folders or files found in Google Drive.")
                break
            else:
                for item in items:
                    all_files.append(item)

                page_token = results.get("nextPageToken")
                if not page_token:
                    break
        return all_files


    def delete_google_files(self,file_or_folder_id):
        """Delete a file or folder in Google Drive by ID."""
        try:
            self.drive_service.files().delete(fileId=file_or_folder_id).execute()
            print(f"Successfully deleted file/folder with ID: {file_or_folder_id}")
        except Exception as e:
            print(f"Error deleting file/folder with ID: {file_or_folder_id}")
            print(f"Error details: {str(e)}")

    def spider_closed(self, spider):
        all_files = self.list_folder()
        for i in range(len(all_files)):
            id = all_files[i]["id"]
            name = all_files[i]["name"]
            if name=="output.csv":
                self.delete_google_files(id)
                break    

        file_metadata = {
            "name": "output.csv",
            "title": "output.csv",
            "parents": [{"id": "1Q29IIzqCFFWXR1uqetdtXNqJjX4ApqFn"}],
            "mimeType": "text/csv",
        }
        media = MediaFileUpload("output.csv", mimetype="text/csv")
        file = (
            self.drive_service.files()
            .insert(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        
        new_permission = {
            "value": "jarrypatel2278@gmail.com",
            "type": "user",
            "role": "reader",
        }
        permission = (
            self.drive_service.permissions()
            .insert(fileId=file["id"], body=new_permission)
            .execute()
        )
        print("File ID:", file.get("id"))