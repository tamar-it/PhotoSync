from google_photos_auth import get_google_photos_credentials
from googleapiclient.discovery import build
import os
import sys
from urllib.request import pathname2url
import requests
if sys.version_info.major == 3 and sys.version_info.minor >= 10:
        import collections
        setattr(collections, "MutableMapping", collections.abc.MutableMapping)


# Setup credentials and service
SCOPES = [
    'https://www.googleapis.com/auth/photoslibrary.appendonly',
    'https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata',
    'https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata'
]
creds = get_google_photos_credentials(scopes=SCOPES)
service = build('photoslibrary', 'v1', credentials=creds)
sync_directory = os.path.expanduser("~/Pictures")

def uploadPhoto(album_id, photo_name, description=None):
    headers = {
        'Authorization': "Bearer " + creds.token,
        'Content-Type': 'application/octet-stream',
        'X-Goog-Upload-File-Name': os.path.basename(photo_name),
        'X-Goog-Upload-Protocol': "raw",
    }
    with open(photo_name, "rb") as photo_file:
        media = photo_file.read()
    body = {"albumId": album_id, "newMediaItems": [{
        'description': description if description is not None else os.path.basename(photo_name)
    }]}
    try:
        print("uploading {}".format(photo_name))
        upload_url = 'https://photoslibrary.googleapis.com/v1/uploads'
        response = requests.post(upload_url, data=media, headers=headers)
        token = response.content
    except Exception as err:
        print("Error uploading {}: {}".format(photo_name, err))
        return
    try:
        if token:
            if album_id == "-":
                body.pop("albumId")
            body["newMediaItems"][0]["simpleMediaItem"] = {"uploadToken": token.decode('utf8')}
            media_result = service.mediaItems().batchCreate(body=body).execute()
            print("\tFile {} status {}".format(photo_name.strip(sync_directory), media_result['newMediaItemResults'][0]['status']))
    except Exception as err:
        print("Error adding media item {}: {}".format(description if description is not None else os.path.basename(photo_name), err))

if len(sys.argv) > 2:
    uploadPhoto(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None)
