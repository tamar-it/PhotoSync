from google_photos_auth import get_google_photos_credentials
from googleapiclient.discovery import build
from apiclient.http import BatchHttpRequest
import threading
import os
from urllib.request import pathname2url
import sys
import mimetypes
from PIL import Image
import datetime
from PIL.ExifTags import TAGS
from PIL import *
from io import BytesIO
import piexif
import multiprocessing
import time
import googleapiclient.errors
import logging
import requests  # Import requests for large video uploads

# Setup logger
logger = logging.getLogger("PhotoSync")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def get_exif_creation_date(path):
    try:
        image = Image.open(path)
    except UnidentifiedImageError:
        logger.error(f"Error: Unable to open image {path}. It may not be a valid image file.")
        return None
    try
        exif_data = image._getexif()
    except:
        return None
    
    if not exif_data:
        return None

    for tag_id, value in exif_data.items():
        tag = TAGS.get(tag_id, tag_id)
        if tag == 'DateTimeOriginal':
            return datetime.datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
    return None

def inject_exif_datetime(image_bytes: bytes, datetime_str: str) -> bytes:
    """Injects EXIF DateTimeOriginal into image bytes in memory."""
    img = Image.open(BytesIO(image_bytes))

    # Create EXIF data
    exif_dict = {"Exif": {piexif.ExifIFD.DateTimeOriginal: datetime_str}}
    exif_bytes = piexif.dump(exif_dict)

    # Save image to memory with EXIF
    output = BytesIO()
    img.save(output, format="JPEG", exif=exif_bytes)
    return output.getvalue()

debug = False

if sys.version_info.major == 3 and sys.version_info.minor >= 10:
        import collections
        setattr(collections, "MutableMapping", collections.abc.MutableMapping)

class PhotoSync:
    def __init__(self, sync_directory='~/Pictures', dry_run=False, large_file_threshold=10 * 1024 * 1024):  # 10MB default
        # Setup credentials
        SCOPES = [
            'https://www.googleapis.com/auth/photoslibrary.appendonly',
            'https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata',
            'https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata'
        ]
        self.creds = get_google_photos_credentials(scopes=SCOPES)
        self.service = build('photoslibrary', 'v1', credentials=self.creds, static_discovery=False)
        self.sync_directory = os.path.expanduser(sync_directory)
        self.photos = {}
        self.albums = self.listAlbums()
        self.dry_run = dry_run
        self.large_file_threshold = large_file_threshold  # Files larger than this will be uploaded using uploadLargeVideo

    def uploadPhotoToLibrary(self, photo_name, description=None, photo_date=None):
        """
        Upload a single photo to the Google Photos library.
            :param photo_name: The path to the photo file.
            :param description: Optional description for the photo.
        """
        if self.dry_run:
            logger.info(f"[Dry Run] Would upload {photo_name} to library with description '{description}'")
            return "dry_run_token"
        headers = {'Authorization': "Bearer " + self.creds.token,
                   'Content-Type': 'application/octet-stream',
                   'X-Goog-Upload-File-Name': '"' + pathname2url(os.path.basename(photo_name)) + '"',
                   'X-Goog-Upload-Protocol': "raw",
        }
        try:
            logger.info(f"Uploading {photo_name}")
            with open(photo_name, "rb") as photo_file:
                media = photo_file.read()
            if photo_date is not None:
                media = inject_exif_datetime(media, photo_date.strftime("%Y:%m:%d %H:%M:%S"))
            token = self.service._http.request('https://photoslibrary.googleapis.com/v1/uploads', method='POST', body=media, headers=headers)
            body = {"newMediaItems": [{'description': description if description is not None else os.path.basename(photo_name), "simpleMediaItem": {"uploadToken": token[1].decode('utf8')}}]}
            media_result = self.safe_batch_create(body)
            if 'newMediaItemResults' in media_result and media_result['newMediaItemResults'] and media_result['newMediaItemResults'][0]['status']['message'] == 'Success':
                logger.info(f"\tFile {photo_name.strip(self.sync_directory)} status {media_result['newMediaItemResults'][0]['status']}")
                return token[1].decode('utf8')
            else:
                logger.error(f"Error uploading {photo_name.strip(self.sync_directory)}: {media_result}")
        except Exception as err:
            logger.error(f"Error uploading {photo_name.strip(self.sync_directory)}\t{err}")
        return None

    def addPhotoToAlbum(self, album_id, photo_token, description=None):
        if self.dry_run:
            logger.info(f"[Dry Run] Would add photo {description} (token: {photo_token}) to album {album_id}")
            return
        logger.info(f"Adding photo {description} to album {album_id}")
        """ Add a photo to a specific album in Google Photos.
            :param album_id: The ID of the album to add the photo to.
            :param photo_id: The ID of the photo to add.
            :param description: Optional description for the photo.
        """
        if not album_id:
            logger.warning("No album ID provided, skipping adding photo to album.")
            return
        if photo_token is not None:
            logger.info(f"Adding photo {description} to album {album_id} {self.albums.get(album_id, '')}")
            body = {"albumId": album_id, "newMediaItems": [{"simpleMediaItem": {"uploadToken": photo_token}}]}
            try:
                media_result = self.safe_batch_create(body)
                logger.info(f"\tFile {media_result['newMediaItemResults'][0]['mediaItem']['description']} status {media_result['newMediaItemResults'][0]['status']}")
            except Exception as err:
                logger.error(f"Error adding media item {description if description is not None else os.path.basename(photo_token)}: {err}")

    def uploadPhotoToAlbum(self, album_id, photo_name, description=None):
        if self.dry_run:
            logger.info(f"[Dry Run] Would upload {photo_name} to album {album_id} with description '{description}'")
            return
        if not os.path.exists(photo_name):
            logger.warning(f"Photo {photo_name} does not exist.")
            return
        
        photo_date = get_exif_creation_date(photo_name)
        if photo_date is None:
            photo_date = datetime.datetime.fromtimestamp(os.path.getmtime(photo_name))
        
        file_size = os.path.getsize(photo_name)
        if file_size > self.large_file_threshold:
            logger.info(f"File {photo_name} is larger than {self.large_file_threshold} bytes, using uploadLargeVideo")
            photo_token = self.uploadLargeVideo(album_id, photo_name, description, photo_date)
        else:
            logger.info(f"File {photo_name} is smaller than {self.large_file_threshold} bytes, using uploadPhotoToLibrary")
            # Upload photo to library and get the token
            photo_token = self.uploadPhotoToLibrary(photo_name, description, photo_date)
        if photo_token is None:
            logger.error(f"Failed to upload photo {photo_name}, skipping adding to album.")
            return

        photo_year = photo_date.strftime('%Y')
        
        logger.info(f"Preparing to upload photo {photo_name} to album {album_id} for year {photo_year}")
        if photo_token is not None:
            if not album_id == "":
                self.addPhotoToAlbum(album_id, photo_token, description)
            if not album_id == self.albums.get(photo_year):
                if not photo_year in self.albums:
                    logger.info(f"Creating album for year {photo_year}")
                    self.createAlbum(photo_year)
                if photo_year in self.albums:
                    self.addPhotoToAlbum(self.albums.get(photo_year), photo_token, description)
                else:
                    logger.warning(f"Year album {photo_year} does not exist, skipping adding photo to year album.")

    def readPhotosInAlbum(self, album_id):
        """
        Read photos in a specific album.
            :param album_id: The ID of the album to read photos from.
        """
        if album_id not in self.photos:
            self.photos[album_id] = []
            read_photos = True
            search_album = {"pageSize": 100, "albumId": album_id}
            photos_in_album = self.service.mediaItems().search(body=search_album).execute()
            while read_photos and "mediaItems" in photos_in_album:
                #print(photos_in_album)
                self.photos[album_id] += [photo.get("description", photo.get("filename")) for photo in photos_in_album.get("mediaItems")]
                search_album["pageToken"] = photos_in_album.get("nextPageToken")
                if search_album["pageToken"] is None:
                    read_photos = False
                else:
                    photos_in_album = self.service.mediaItems().search(body=search_album).execute()
    
    def uploadDirectory(self, album_id, path, subdir, times_in=0, force=False):
        logger.info(f"Uploading {album_id} {os.path.join(path, subdir)}")
        if(times_in == 0):
            self.readPhotosInAlbum(album_id)
            # read photos in album if album is not new                    
        localpath = os.path.join(self.sync_directory, subdir)
        pool = multiprocessing.Pool(processes=2, maxtasksperchild=2)
        upload_tasks = []
        for image_file in os.listdir(localpath):
            if os.path.isdir(os.path.join(localpath, image_file)):
                if debug:
                    logger.info(f"Found subdirectory: {image_file}")
                if image_file == '.' or image_file == '..':
                    continue
                self.uploadDirectory(album_id, path, os.path.join(subdir, image_file), times_in+1, force)
            elif os.path.isfile(os.path.join(localpath, image_file)):
                mime_type, _ = mimetypes.guess_type(image_file)
                if mime_type is None or not (mime_type.startswith('image/') or mime_type.startswith('video/') or mime_type == 'image/raw'):
                    if debug:
                        logger.info(f"Skipping non-image file: {image_file}")
                    continue
                image_description = "-".join([subdir] + [image_file])
                image_filename = os.path.join(localpath, image_file)
                if not force and (image_description in self.photos[album_id] or image_file in self.photos[album_id] or pathname2url(image_file) in self.photos[album_id]):
                    if debug:
                        logger.info(f"File {image_file} already exists in photos, skipping upload.")
                    continue
                logger.info(f"media {image_filename} / {image_file} ==== {image_description}")
                if self.dry_run:
                    logger.info(f"[Dry Run] Would upload {image_filename} to album {album_id} with description '{image_description}'")
                else:
                    upload_tasks.append((album_id, image_filename, image_description))
        if upload_tasks and not self.dry_run:
            pool.starmap(self.uploadPhotoToAlbum, upload_tasks)
            pool.close()
            pool.join()
        if times_in == 0:
            self.photos.pop(album_id)

    def syncDirectory(self, subdir=None, force=False):
        logger.info(f"Found {len(self.albums)} albums")
        if subdir is not None:
            logger.info(f"Syncing subdirectory: {subdir}")
            directory = os.path.join(self.sync_directory, subdir)
            if debug:
                logger.info(f"subdir: {subdir}, directory: {directory}")
            if not subdir in self.albums:
                logger.info(f"Creating album for subdir: {subdir}")
                if not self.dry_run:
                    self.albums[subdir] = self.createAlbum(subdir)
            return self.uploadDirectory(self.albums.get(subdir, ''), self.sync_directory, subdir, 0, force)
        if not os.path.exists(self.sync_directory):
            logger.error(f"Directory {self.sync_directory} does not exist, exiting.")
            return
        times = 0
        sync_pool = multiprocessing.Pool(processes=2, maxtasksperchild=2)
        photos_pool = multiprocessing.Pool(processes=2, maxtasksperchild=2)
        sync_task = []
        photo_task = []
        for file_name in os.listdir(self.sync_directory):
            if os.path.isdir(os.path.join(self.sync_directory, file_name)):
                logger.info(f"Searching for '{file_name}' in albums")
                album_id = self.albums.get(file_name)
                if album_id is None:
                    if not self.dry_run:
                        self.albums[file_name] = album_id = self.createAlbum(file_name)
                        self.photos[album_id] = []
                if self.dry_run:
                    logger.info(f"[Dry Run] Would sync directory '{file_name}' to album {album_id}")
                else:
                    self.uploadDirectory(album_id, self.sync_directory, file_name, 0, force)
                    logger.info(f"calling thread self.uploadDirectory {album_id} [] {file_name} 0")
                    sync_task.append((album_id, [], file_name, 0, force))
                times += 1
            else:
                if debug:
                    logger.info(f"Found file: {file_name}")
                mime_type, _ = mimetypes.guess_type(file_name)
                if (mime_type.startswith('image/') and not mime_type == 'image/raw') or mime_type.startswith('video/'):
                    if self.dry_run:
                        logger.info(f"[Dry Run] Would upload {file_name} to library")
                    else:
                        photo_task.append(('', os.path.join(self.sync_directory, file_name), None))
        if sync_task and not self.dry_run:
            sync_pool.starmap(self.uploadDirectory, sync_task)
            sync_pool.close()
            sync_pool.join()
        if photo_task and not self.dry_run:
            photos_pool.starmap(self.uploadPhotoToAlbum, photo_task)
            photos_pool.close()
            photos_pool.join()

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
        """ thread safe create album """
        mutex = threading.Lock()
        with mutex:
            if album_name in self.albums:
                logger.info(f"Album {album_name} already exists with ID {self.albums[album_name]}")
                return self.albums[album_name]
            if self.dry_run:
                logger.info(f"[Dry Run] Would create album '{album_name}'")
                # Simulate an album ID for dry run
                fake_id = f"dry_run_album_{album_name}"
                self.albums[album_name] = fake_id
                return fake_id
            results = self.service.albums().create(body={'album':{'title':album_name}}).execute()
            logger.info("Album {a[title]}, ID: {a[id]}, Writeable: {a[isWriteable]}, URL: {a[productUrl]}".format(a=results))
            if results and 'id' in results:
                self.albums[album_name] = results["id"]
            return results["id"]
        mutex.release()

    def uploadPhoto(self,album_id,photo_name,description=None):
        #batch = BatchHttpRequest()
        headers = {
            'Authorization': "Bearer " + self.creds.token,
            'Content-Type': 'application/octet-stream',
            'X-Goog-Upload-File-Name': '"' + pathname2url(photo_name) + '"',
            'X-Goog-Upload-Protocol': "raw",
        }
        try:
            logger.info(f"Uploading {photo_name}")
            with open(photo_name,"rb") as photo_file:
                media = photo_file.read()
                token=self.service._http.request('https://photoslibrary.googleapis.com/v1/uploads', method='POST', body=media,headers=headers)
            body = {"albumId":album_id,"newMediaItems":[{'description':description if description is not None else os.path.basename(photo_name),"simpleMediaItem": {"uploadToken": token[1].decode('utf8')}}]}
            media_result = self.safe_batch_create(body=body)
            logger.info(f"\tFile {photo_name.strip(self.sync_directory)} status {media_result['newMediaItemResults'][0]['status']}")
        except Exception as e:
            logger.error(f"Error uploading {photo_name.strip(self.sync_directory)}\t{e}")
        return

    def printMediaItem(self, media_item):
        """
        Print details of a media item.
        :param media_item: The media item to print.
        """
        print(f"Media Item: {media_item.get('filename', 'Unknown')}")
        print(f"  ID: {media_item.get('id', 'Unknown')}")
        print(f"  Description: {media_item.get('description', 'No description')}")
        print(f"  Creation Time: {media_item.get('mediaMetadata', {}).get('creationTime', 'Unknown')}")
        if 'photo' in media_item.get('mediaMetadata', {}):
             photo_metadata = media_item['mediaMetadata']['photo']
             print(f"  Camera Make: {photo_metadata.get('cameraMake', 'Unknown')}")
             print(f"  Camera Model: {photo_metadata.get('cameraModel', 'Unknown')}")
        if 'video' in media_item.get('mediaMetadata', {}):
             video_metadata = media_item['mediaMetadata']['video']
             print(f"  Video Duration: {video_metadata.get('durationMillis', 'Unknown')} ms")

    def albumActions(self, album_id, action):
        """
        Perform actions on the album.
        :param album_id: The ID of the album to perform actions on.
        :param action: The action to perform (e.g., 'delete', 'share').
        """
        if action == 'delete':
            self.service.albums().delete(albumId=album_id).execute()
            logger.info(f"Album {album_id} deleted.")
        elif action == 'info':
            # Get album info
            try:
                try:
                    album = self.service.albums().get(albumId=album_id).execute()
                except:
                    albums = self.listAlbums()
                    album_id = albums.get(album_id)
                    album = self.service.albums().get(albumId=album_id).execute()
                if not album:
                    logger.warning(f"Album {album_id} not found.")
                    return
                print(f"Album ID: {album.get('id')}")
                print(f"Title: {album.get('title')}")
                print(f"Description: {album.get('description', 'No description')}")
                print(f"Product URL: {album.get('productUrl')}")
                print(f"Writeable: {album.get('isWriteable')}")
                print(f"Media items count: {album.get('mediaItemsCount', 0)}")
                print(f"Cover photo base URL: {album.get('coverPhotoBaseUrl')}")
                print(f"Cover photo media item ID: {album.get('coverPhotoMediaItemId')}")
                print(f"Created time: {album.get('createdTime')}")
                print(f"Updated time: {album.get('updatedTime')}")
                print(f"Shareable: {album.get('shareable')}")
                print(f"Shared album: {album.get('sharedAlbum')}")
                print(f"Share token: {album.get('shareToken')}")
                print(f"Shareable URL: {album.get('shareableUrl')}")
                print(f"Owner: {album.get('owner', {}).get('displayName', 'Unknown')} ({album.get('owner', {}).get('emailAddress', 'Unknown')})")
            except Exception as e:
                logger.error(f"Error retrieving album info: {e}")
        elif action == 'photos':
            # List photos in the album
            try:
                results = self.service.mediaItems().search(body={"albumId": album_id, "pageSize": 100}).execute()
            except:
                 albums = self.listAlbums()
                 album_id = albums.get(album_id)
                 results = self.service.mediaItems().search(body={"albumId": album_id, "pageSize": 100}).execute()
            photos = []
            if 'mediaItems' in results:
                photos.extend(results.get('mediaItems'))
            while 'nextPageToken' in results:
                next_page_token = results.get('nextPageToken')
                results = self.service.mediaItems().search(body={"albumId": album_id, "pageSize":100, "pageToken": next_page_token}).execute()
                if 'mediaItems' in results:
                     photos.extend(results.get('mediaItems'))

            for photo in photos:
                 self.printMediaItem(photo)
        else:
            logger.warning(f"Unknown action: {action}")

    def safe_batch_create(self, body, max_retries=5):
        for attempt in range(max_retries):
            try:
                return self.service.mediaItems().batchCreate(body=body).execute()
            except googleapiclient.errors.HttpError as e:
                if e.resp.status == 429:
                    wait = 2 ** attempt
                    logger.warning(f"Quota exceeded, retrying in {wait} seconds...")
                    time.sleep(wait)
                else:
                    raise
        raise Exception("Too many retries for batchCreate")

    def uploadLargeVideo(self, album_id, video_path, description=None, video_date=None, chunk_size=8 * 1024 * 1024):
        """
        Upload a large video file to Google Photos by streaming it in chunks.
        Note: Google Photos API does not support resumable uploads, but streaming avoids loading the whole file into memory.
        :param album_id: The album ID to add the video to.
        :param video_path: Path to the video file.
        :param description: Optional description.
        :param chunk_size: Size of chunks to stream (default 8MB).
        """
        if self.dry_run:
            logger.info(f"[Dry Run] Would upload large video {video_path} to album {album_id} with description '{description}'")
            return

        headers = {
            'Authorization': "Bearer " + self.creds.token,
            'Content-Type': 'application/octet-stream',
            'X-Goog-Upload-File-Name': '"' + pathname2url(os.path.basename(video_path)) + '"',
            'X-Goog-Upload-Protocol': "raw",
        }
        upload_url = 'https://photoslibrary.googleapis.com/v1/uploads'
        try:
            logger.info(f"Uploading large video {video_path} (streaming in chunks)")
            with open(video_path, "rb") as video_file:
                response = requests.post(upload_url, data=video_file, headers=headers, timeout=1800)
            if response.status_code != 200:
                logger.error(f"Failed to upload video {video_path}: {response.status_code} {response.text}")
                return
            upload_token = response.content.decode('utf-8')
            body = {
                "albumId": album_id,
                "newMediaItems": [{
                    'description': description if description else os.path.basename(video_path),
                    "simpleMediaItem": {"uploadToken": upload_token}
                }]
            }
            media_result = self.safe_batch_create(body)
            if 'newMediaItemResults' in media_result and media_result['newMediaItemResults'] and media_result['newMediaItemResults'][0]['status']['message'] == 'Success':
                logger.info(f"\tLarge video {video_path.strip(self.sync_directory)} status {media_result['newMediaItemResults'][0]['status']}")
            else:
                logger.error(f"Error uploading large video {video_path.strip(self.sync_directory)}: {media_result}")
        except Exception as err:
            logger.error(f"Error uploading large video {video_path.strip(self.sync_directory)}\t{err}")
        return upload_token
        

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Download Google Photos hierarchically by year/month/day/image-name, avoid duplicates, and put in year album.")
    parser.add_argument('--source', type=str, default=os.path.expanduser('~/Pictures'), help='Source root directory')
    parser.add_argument('--dry-run', action='store_true', help='Dry run: only print actions, do not download')
    parser.add_argument('--list', action='store_true', help='List Albumes: List all created albumes')
    parser.add_argument('--album-info', type=str, help='Get album info by ID or URL')
    parser.add_argument('--album-photos', type=str, help='List photos in the album by ID or URL')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    parser.add_argument('directory', nargs='?', help='Directory to sync, if not specified, use the default sync directory')
    parser.add_argument('--force', action='store_true', help='Force upload even if the photo already exists in the album')
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
        debug = True

    # Pass dry_run to PhotoSync
    photo_sync = PhotoSync(args.source if args.source else '~/Pictures', dry_run=args.dry_run)

    if args.list:
        albums = photo_sync.listAlbums()
        print("Found {} albums:".format(len(albums)))
        for title, album_id in albums.items():
               print(f"{title} - {album_id}")
        sys.exit(0)
    if args.album_photos:
        album_id = args.album_photos
        if album_id.startswith('https://photos.app.goo.gl/'):
             album_id = album_id.split('/')[-1]
        logger.info(f"Album ID: {album_id}")
        photo_sync.albumActions(album_id, 'photos')
        sys.exit(0)
    if args.album_info:
        album_id = args.album_info
        if album_id.startswith('https://photos.app.goo.gl/'):
            album_id = album_id.split('/')[-1]
        logger.info(f"Album ID: {album_id}")
        photo_sync.albumActions(album_id, 'info')
        sys.exit(0)
    else:
        photo_sync.syncDirectory(args.directory if args.directory else None, force=args.force)
