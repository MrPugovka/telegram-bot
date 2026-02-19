import io
import os
import pickle
import logging
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/drive"]
ROOT_FOLDER_ID = "1o3CTuRogOHSd8CxlqdPOMliUYTn7AGkl"
CONTRACTS_FOLDER_ID = "1fVh7gqQiOeSOjbW68aTQ6Z-t8pm19634"

def get_drive_service():
    creds = None
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as f:
            creds = pickle.load(f)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.pickle", "wb") as f:
            pickle.dump(creds, f)
    return build("drive", "v3", credentials=creds)

def get_latest_video(folder_name):
    try:
        drive = get_drive_service()
        query = f"name = '{folder_name}' and '{ROOT_FOLDER_ID}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        results = drive.files().list(q=query, fields="files(id)").execute()
        folders = results.get('files', [])
        if not folders: 
            return None
        
        folder_id = folders[0]['id']
        
        query = f"'{folder_id}' in parents and trashed = false"
        results = drive.files().list(
            q=query, 
            orderBy="createdTime desc", 
            pageSize=1, 
            fields="files(id, name)"
        ).execute()
        
        files = results.get('files', [])
        if not files: 
            return None
        
        file_id = files[0]['id']
        request = drive.files().get_media(fileId=file_id)
        
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        
        fh.seek(0)
        return fh.read()
    except Exception as e:
        logging.error(f"Ошибка Drive: {e}")
        return None

def get_or_create_folder(drive, name, parent_id):
    safe_name = name.replace("'", "\\'").replace('"', '\\"').replace("\\", "\\\\")
    
    query = (
        f"name='{safe_name}' and "
        f"mimeType='application/vnd.google-apps.folder' and "
        f"'{parent_id}' in parents and trashed=false"
    )
    result = drive.files().list(q=query, fields="files(id)").execute()
    files = result.get("files", [])
    if files:
        return files[0]["id"]

    folder = drive.files().create(
        body={
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id]
        },
        fields="id"
    ).execute()

    return folder["id"]

from googleapiclient.http import MediaFileUpload
import tempfile
import os

def upload_video(file_bytes: bytes, filename: str, folder_name: str):
    tmp_path = None
    try:
        drive = get_drive_service()
        folder_id = get_or_create_folder(drive, folder_name, ROOT_FOLDER_ID)
        
        # Используем временный файл для больших видео
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
            if isinstance(file_bytes, bytes):
                tmp.write(file_bytes)
            else:
                tmp.write(file_bytes.read())
            tmp_path = tmp.name

        media = MediaFileUpload(
            tmp_path,
            mimetype="video/mp4",
            resumable=True,
            chunksize=25 * 1024 * 1024  # Увеличили размер чанка до 25MB
        )

        request = drive.files().create(
            body={"name": filename, "parents": [folder_id]},
            media_body=media,
            fields="id"
        )
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                logging.info(f"Загружено {int(status.progress() * 100)}%")

        os.remove(tmp_path)
        return True

    except Exception as e:
        logging.error(f"Ошибка загрузки видео: {e}")
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        return False

def get_or_create_folder_for_bike(folder_name: str) -> str | None:
    """Create or get a folder for a bike in the ROOT_FOLDER_ID directory."""
    try:
        drive = get_drive_service()
        return get_or_create_folder(drive, folder_name, ROOT_FOLDER_ID)
    except Exception as e:
        logging.error(f"Ошибка создания/получения папки для байка: {e}")
        return None

def check_folder_exists(folder_name: str) -> str | None:
    """Check if a folder exists in ROOT_FOLDER_ID directory. Returns folder_id or None."""
    try:
        drive = get_drive_service()
        safe_name = folder_name.replace("'", "\\'").replace('"', '\\"').replace("\\", "\\\\")
        
        query = (
            f"name='{safe_name}' and "
            f"mimeType='application/vnd.google-apps.folder' and "
            f"'{ROOT_FOLDER_ID}' in parents and trashed=false"
        )
        result = drive.files().list(q=query, fields="files(id)").execute()
        files = result.get("files", [])
        if files:
            return files[0]["id"]
        return None
    except Exception as e:
        logging.error(f"Ошибка проверки существования папки: {e}")
        return None

def upload_contract_photo(file_bytes: bytes, filename: str, folder_name: str, folder_id: str | None = None):
    try:
        drive = get_drive_service()
        if not folder_id:
            folder_id = get_or_create_folder(drive, folder_name, CONTRACTS_FOLDER_ID)
        
        media = MediaIoBaseUpload(
            io.BytesIO(file_bytes),
            mimetype="image/jpeg",
            resumable=True
        )

        drive.files().create(
            body={"name": filename, "parents": [folder_id]},
            media_body=media,
            fields="id"
        ).execute()

        return folder_id
    except Exception as e:
        logging.error(f"Ошибка загрузки фото договора: {e}")
        return None
