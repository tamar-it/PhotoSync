from google_photos_auth import get_google_photos_credentials
from apiclient.http import BatchHttpRequest
import threading
from httplib2 import Http
from oauth2client import file, client, tools
import os
from urllib.request import pathname2url
from time import sleep
import subprocess
import sys
debug = False

if sys.version_info.major == 3 and sys.version_info.minor >= 10:
        import collections
        setattr(collections, "MutableMapping", collections.abc.MutableMapping)

class PhotoSync:
    def __init__(self):
        # Setup credentials
        SCOPES = [
            'https://www.googleapis.com/auth/photoslibrary.appendonly',
            'https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata',
            'https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata'
        ]
        creds = get_google_photos_credentials(scopes=SCOPES)
        from googleapiclient.discovery import build
        self.service = build('photoslibrary', 'v1', credentials=creds)
        self.sync_directory = os.path.expanduser("~/Pictures")
        self.photos = {}

    def uploadDirectory(self, album_id, path, subdir, times_in=0):
        print("uploading {} {} {}".format(album_id, '/'.join(path), subdir))

        localpath = os.path.join(self.sync_directory, os.path.sep.join(path), subdir)
        times = 0
        for image_file in os.listdir(localpath):
            if os.path.isdir(os.path.join(localpath, image_file)):
                # Recursive call to uploadDirectory if the item is a directory
                print("Found subdirectory: {}".format(image_file)) if debug else None
                if image_file == '.'or image_file == '..':
                    continue               
                self.uploadDirectory(album_id, path + [subdir,], image_file, times_in+1)
            elif image_file.endswith(('.jpg','.JPG','.png','.PNG','.gif','.GIF','.jpeg')):
                image_description = "-".join(path + [ image_file ])
                image_filename = os.path.join(localpath, image_file)
                print("photo {} / {} ==== {}".format(image_filename, image_file ,image_description))
                if set((image_filename, image_description, image_file, pathname2url(image_file))) & set(self.photos[album_id]) == set():
                    #self.uploadPhoto(album_id, image_filename, image_description)
                    #subprocess.Popen(['python','UploadPhotoToAlbume.py',album_id, image_filename, image_description])
                    subprocess.run(['python','UploadPhotoToAlbume.py',album_id, image_filename, image_description])
                    """imgThread = threading.Thread(target=self.uploadPhoto, args=(album_id, image_filename, image_description))
                    imgThread.start()"""
                    times += 1
                    if(times >= 3):
                        sleep(10)
                        times = 0
        if times_in == 0:
            self.photos.pop(album_id)

    def syncDirectory(self,subdir = None):
        albums = self.listAlbums()
        print("Found {} albums".format(len(albums)))
        times = 0
        for file_name in os.listdir(self.sync_directory):
            if subdir is not None and subdir != file_name:
                print("Skipping {} as it is not the subdir {}".format(file_name, subdir)) if debug else None
                continue
            if os.path.isdir(os.path.join(self.sync_directory, file_name)):
                # find album's id
                print("Searching for '{}' in albums".format(file_name))
                album_id =  albums.get(file_name)
                if album_id is None:
                    album_id = self.createAlbum(file_name)
                    self.photos[album_id] = []
                else:
                    # Read photos in album if ulbum is not new
                    if album_id not in self.photos:
                        self.photos[album_id] = []
                        read_photos = True
                        search_album = {"pageSize": 100, "albumId": album_id}
                        photos_in_album = self.service.mediaItems().search(body=search_album).execute()
                        while read_photos and "mediaItems" in photos_in_album:
                            #print(photos_in_album)
                            self.photos[album_id] += [ photo.get("description", photo.get("filename")) for photo in
                                                        photos_in_album.get("mediaItems")]
                            search_album["pageToken"] = photos_in_album.get("nextPageToken")
                            if search_album["pageToken"] is None:
                                read_photos = False
                            else:
                                photos_in_album = self.service.mediaItems().search(body=search_album).execute()
                    for p in  self.photos[album_id]:
                        print("In Album: {}".format(p))
                #self.uploadDirectory(album_id, [], file_name)
                #self.photos.pop(album_id)
                print("calling thread self.uploadDirectory {} [] {} 0".format(album_id,file_name))
                dirThread = threading.Thread(target=self.uploadDirectory, args=(album_id,[],file_name,0))
                dirThread.start()
                """times += 1
                if (times == 2):
                    sleep(30)
                    times = 0"""
            elif file_name.endswith(('.jpg','.JPG','.png','.PNG', '.gif', '.GIF','.jpeg')):
                print("Openning subprocess UploadPhotoToAlbume.py {} {}".format(os.path.join(self.sync_directory,file_name), file_name))
                subprocess.Popen(['python','UploadPhotoToAlbume.py',"-",  os.path.join(self.sync_directory,file_name), file_name])
                #imgThread = threading.Thread(target=self.uploadPhoto, args=('', os.path.join(self.sync_directory,file_name)))
                #imgThread.start()


    def listAlbums(self):
        # Call the Photo v1 API
        results = self.service.albums().list(pageSize=50, fields="nextPageToken,albums(id,title)").execute()
        items = results.get('albums', [])
        page_token = results.get('nextPageToken')
        while page_token is not None:
            results = self.service.albums().list(pageSize=50, fields="nextPageToken,albums(id,title)", pageToken=page_token).execute()
            items += results.get('albums', [])
            page_token = results.get('nextPageToken')

        results = self.service.albums().list(fields="nextPageToken,albums(id,title)").execute()
        albums = {}
        for album in items:
            albums[album.get("title")] = album.get("id")
        return albums

    def createAlbum(self,album_name):
        results = self.service.albums().create(body={'album':{'title':album_name}}).execute()
        print("Album {a[title]}, ID: {a[id]}, Writeable: {a[isWriteable]}, URL: {a[productUrl]}".format(a=results))
        return results["id"]

    def uploadPhoto(self,album_id,photo_name,description=None):
        #batch = BatchHttpRequest()
        headers = {
            'Authorization': "Bearer " + self.service._http.request.credentials.access_token,
            'Content-Type': 'application/octet-stream',
            'X-Goog-Upload-File-Name': '"' + pathname2url(photo_name) + '"',
            'X-Goog-Upload-Protocol': "raw",
        }
        try:
            print("uploading {}".format(photo_name))
            with open(photo_name,"rb") as photo_file:
                media = photo_file.read()
                token=self.service._http.request('https://photoslibrary.googleapis.com/v1/uploads', method='POST', body=media,headers=headers)
            body = {"albumId":album_id,"newMediaItems":[{'description':description if description is not None else os.path.basename(photo_name),"simpleMediaItem": {"uploadToken": token[1].decode('utf8')}}]}
            media_result = self.service.mediaItems().batchCreate(body=body).execute()
            print("\tFile {} status {}".format(photo_name.strip(self.sync_directory), media_result['newMediaItemResults'][0]['status']))
        except Exception as err:
            print("error uploading {}\n{}".format(photo_name.strip(self.sync_directory),err))


photo_sync = PhotoSync()

photo_sync.syncDirectory(sys.argv[1] if len(sys.argv)>1 else None)

