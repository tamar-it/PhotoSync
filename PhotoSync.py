from __future__ import print_function
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
from collections import MutableMapping

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
    except IOError:
        logger.error("Error: Unable to open image {}. It may not be a valid image file.".format(path))
        return None
    exif_data = image._getexif()
    if not exif_data:
        return None

    for tag_id, value in exif_data.items():
        tag = TAGS.get(tag_id, tag_id)
        if tag == 'DateTimeOriginal':
            return datetime.datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
    return None

def inject_exif_datetime(image_bytes, datetime_str):
    """Injects EXIF DateTimeOriginal into image bytes in memory."""
    img = Image.open(BytesIO(image_bytes))

    # Create EXIF data
    exif_dict = {"Exif": {piexif.ExifIFD.DateTimeOriginal: datetime_str.encode('utf-8')}}
    exif_bytes = piexif.dump(exif_dict)

    # Save image to memory with EXIF
    output = BytesIO()
    img.save(output, format="JPEG", exif=exif_bytes)
    return output.getvalue()

debug = False

if sys.version_info.major == 3 and sys.version_info.minor >= 10:
        import collections
        setattr(collections, "MutableMapping", collections.abc.MutableMapping)

class PhotoSync(object):
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
            logger.info("[Dry Run] Would upload {} to library with description '{}'".format(photo_name, description))
            return "dry_run_token"
        headers = {'Authorization': "Bearer " + self.creds.token,
                   'Content-Type': 'application/octet-stream',
                   'X-Goog-Upload-File-Name': '"' + pathname2url(os.path.basename(photo_name)) + '"',
                   'X-Goog-Upload-Protocol': "raw",
        }
        try:
            logger.info("Uploading {}".format(photo_name))
            with open(photo_name, "rb") as photo_file:
                media = photo_file.read()
            if photo_date is not None:
                media = inject_exif_datetime(media, photo_date.strftime("%Y:%m:%d %H:%M:%S"))
            token = self.service._http.request('https://photoslibrary.googleapis.com/v1/uploads', method='POST', body=media, headers=headers)
            body = {"newMediaItems": [{'description': description if description is not None else os.path.basename(photo_name), "simpleMediaItem": {"uploadToken": token[1].decode('utf8')}}]}
            media_result = self.safe_batch_create(body)
            if 'newMediaItemResults' in media_result and media_result['newMediaItemResults'] and media_result['newMediaItemResults'][0]['status']['message'] == 'Success':
                logger.info("\tFile {} status {}".format(photo_name.strip(self.sync_directory), media_result['newMediaItemResults'][0]['status']))
                return token[1].decode('utf8')
            else:
                logger.error("Error uploading {}: {}".format(photo_name.strip(self.sync_directory), media_result))
        except Exception as err:
            logger.error("Error uploading {}\t{}".format(photo_name.strip(self.sync_directory), err))
        return None

    def addPhotoToAlbum(self, album_id, photo_token, description=None):
        if self.dry_run:
            logger.info("[Dry Run] Would add photo {} (token: {}) to album {}".format(description, photo_token, album_id))
            return
        logger.info("Adding photo {} to album {}".format(description, album_id))
        """ Add a photo to a specific album in Google Photos.
            :param album_id: The ID of the album to add the photo to.
            :param photo_id: The ID of the photo to add.
            :param description: Optional description for the photo.
        """
        if not album_id:
            logger.warning("No album ID provided, skipping adding photo to album.")
            return
        if photo_token is not None:
            logger.info("Adding photo {} to album {} {}".format(description, album_id, self.albums.get(album_id, '')))
            body = {"albumId": album_id, "newMediaItems": [{"simpleMediaItem": {"uploadToken": photo_token}}]}
            try:
                media_result = self.safe_batch_create(body)
                logger.info("\tFile {} status {}".format(media_result['newMediaItemResults'][0]['mediaItem']['description'], media_result['newMediaItemResults'][0]['status']))
            except Exception as err:
                logger.error("Error adding media item {}: {}".format(description if description is not None else os.path.basename(photo_token), err))

    def uploadPhotoToAlbum(self, album_id, photo_name, description=None):
        if self.dry_run:
            logger.info("[Dry Run] Would upload {} to album {} with description '{}'".format(photo_name, album_id, description))
            return
        if not os.path.exists(photo_name):
            logger.warning("Photo {} does not exist.".format(photo_name))
            return
        
        photo_date = get_exif_creation_date(photo_name)
        if photo_date is None:
            photo_date = datetime.datetime.fromtimestamp(os.path.getmtime(photo_name))
        
        file_size = os.path.getsize(photo_name)
        if file_size > self.large_file_threshold:
            logger.info("File {} is larger than {} bytes, using uploadLargeVideo".format(photo_name, self.large_file_threshold))
            photo_token = self.uploadLargeVideo(album_id, photo_name, description, photo_date)
        else:
            logger.info("File {} is smaller than {} bytes, using uploadPhotoToLibrary".format(photo_name, self.large_file_threshold))
            # Upload photo to library and get the token
            photo_token = self.uploadPhotoToLibrary(photo_name, description, photo_date)
        if photo_token is None:
            logger.error("Failed to upload photo {}, skipping adding to album.".format(photo_name))
            return

        photo_year = photo_date.strftime('%Y')
        
        logger.info("Preparing to upload photo {} to album {} for year {}".format(photo_name, album_id, photo_year))
        if photo_token is not None:
            if not album_id == "":
                self.addPhotoToAlbum(album_id, photo_token, description)
            if not album_id == self.albums.get(photo_year):
                if not photo_year in self.albums:
                    logger.info("Creating album for year {}".format(photo_year))
                    self.createAlbum(photo_year)
                if photo_year in self.albums:
                    self.addPhotoToAlbum(self.albums.get(photo_year), photo_token, description)
                else:
                    logger.warning("Year album {} does not exist, skipping adding photo to year album.".format(photo_year))

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
        logger.info("Uploading {} {}".format(album_id, os.path.join(path, subdir)))
        if(times_in == 0):
            self.readPhotosInAlbum(album_id)
            # read photos in album if album is not new                    
        localpath = os.path.join(self.sync_directory, subdir)
        pool = multiprocessing.Pool(processes=2, maxtasksperchild=2)
        upload_tasks = []
        for image_file in os.listdir(localpath):
            if os.path.isdir(os.path.join(localpath, image_file)):
                if debug:
                    logger.info("Found subdirectory: {}".format(image_file))
                if image_file == '.' or image_file == '..':
                    continue
                self.uploadDirectory(album_id, path, os.path.join(subdir, image_file), times_in+1, force)
            elif os.path.isfile(os.path.join(localpath, image_file)):
                mime_type, _ = mimetypes.guess_type(image_file)
                if mime_type is None or not (mime_type.startswith('image/') or mime_type.startswith('video/') or mime_type == 'image/raw'):
                    if debug:
                        logger.info("Skipping non-image file: {}".format(image_file))
                    continue
                image_description = "-".join([subdir] + [image_file])
                image_filename = os.path.join(localpath, image_file)
                if not force and (image_description in self.photos[album_id] or image_file in self.photos[album_id] or pathname2url(image_file) in self.photos[album_id]):
                    if debug:
                        logger.info("File {} already exists in photos, skipping upload.".format(image_file))
                    continue
                logger.info("media {} / {} ==== {}".format(image_filename, image_file, image_description))
                if self.dry_run:
                    logger.info("[Dry Run] Would upload {} to album {} with description '{}'".format(image_filename, album_id, image_description))
                else:
                    upload_tasks.append((album_id, image_filename, image_description))
        if upload_tasks and not self.dry_run:
            pool.starmap(self.uploadPhotoToAlbum, upload_tasks)
            pool.close()
            pool.join()
        if times_in == 0:
            self.photos.pop(album_id)

    def syncDirectory(self, subdir=None, force=False):
        logger.info("Found {} albums".format(len(self.albums)))
        if subdir is not None:
            logger.info("Syncing subdirectory: {}".format(subdir))
            directory = os.path.join(self.sync_directory, subdir)
            if debug:
                logger.info("subdir: {}, directory: {}".format(subdir, directory))
            if not subdir in self.albums:
                logger.info("Creating album for subdir: {}".format(subdir))
                if not self.dry_run:
                    self.albums[subdir] = self.createAlbum(subdir)
            return self.uploadDirectory(self.albums.get(subdir, ''), self.sync_directory, subdir, 0, force)
        if not os.path.exists(self.sync_directory):
            logger.error("Directory {} does not exist, exiting.".format(self.sync_directory))
            return
        times = 0
        sync_pool = multiprocessing.Pool(processes=2, maxtasksperchild=2)
        photos_pool = multiprocessing.Pool(processes=2, maxtasksperchild=2)
        sync_task = []
        photo_task = []
        for file_name in os.listdir(self.sync_directory):
            if os.path.isdir(os.path.join(self.sync_directory, file_name)):
                logger.info("Searching for '{}' in albums".format(file_name))
                album_id = self.albums.get(file_name)
                if album_id is None:
                    if not self.dry_run:
                        self.albums[file_name] = album_id = self.createAlbum(file_name)
                        self.photos[album_id] = []
                if self.dry_run:
                    logger.info("[Dry Run] Would sync directory '{}' to album {}".format(file_name, album_id))
                else:
                    self.uploadDirectory(album_id, self.sync_directory, file_name, 0, force)
                    logger.info("calling thread self.uploadDirectory {} [] {} 0".format(album_id, file_name))
                    sync_task.append((album_id, [], file_name, 0, force))
                times += 1
            else:
                if debug:
                    logger.info("Found file: {}".format(file_name))
                mime_type, _ = mimetypes.guess_type(file_name)
                if (mime_type.startswith('image/') and not mime_type == 'image/raw') or mime_type.startswith('video/'):
                    if self.dry_run:
                        logger.info("[Dry Run] Would upload {} to library".format(file_name))
                    else:
                        photo_task.append(('', os.path.join(self.sync_directory, file_name), None))
        if sync_task and not self.dry_run:
            sync_pool.starmap(self.uploadDirectory, sync_task)
            sync_pool.close()
            sync_pool.join()
        if photo_task and not self.dry_run:
            photos_pool.starmap(self.uploadPhotoToAlbum, photo_task)

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
                logger.info("Album {} already exists with ID {}".format(album_name, self.albums[album_name]))
                return self.albums[album_name]
            if self.dry_run:
                logger.info("[Dry Run] Would create album '{}'".format(album_name))
                # Simulate an album ID for dry run
                fake_id = "dry_run_album_{}".format(album_name)
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
            logger.info("Uploading {}".format(photo_name))
            with open(photo_name,"rb") as photo_file:
                media = photo_file.read()
                token=self.service._http.request('https://photoslibrary.googleapis.com/v1/uploads', method='POST', body=media,headers=headers)
            body = {"albumId":album_id,"newMediaItems":[{'description':description if description is not None else os.path.basename(photo_name),"simpleMediaItem": {"uploadToken": token[1].decode('utf8')}}]}
            media_result = self.safe_batch_create(body=body)
            logger.info("\tFile {} status {}".format(photo_name.strip(self.sync_directory), media_result['newMediaItemResults'][0]['status']))
        except Exception as e:
            logger.error("Error uploading {}\t{}".format(photo_name.strip(self.sync_directory), e))
        return

    def printMediaItem(self, media_item):
        """
        Print details of a media item.
        :param media_item: The media item to print.
        """
        print("Media Item: {}".format(media_item.get('filename', 'Unknown')))
        print("  ID: {}".format(media_item.get('id', 'Unknown')))
        print("  Description: {}".format(media_item.get('description', 'No description')))
        print("  Creation Time: {}".format(media_item.get('mediaMetadata', {}).get('creationTime', 'Unknown')))
        if 'photo' in media_item.get('mediaMetadata', {}):
             photo_metadata = media_item['mediaMetadata']['photo']
             print("  Camera Make: {}".format(photo_metadata.get('cameraMake', 'Unknown')))
             print("  Camera Model: {}".format(photo_metadata.get('cameraModel', 'Unknown')))
        if 'video' in media_item.get('mediaMetadata', {}):
             video_metadata = media_item['mediaMetadata']['video']
             print("  Video Duration: {} ms".format(video_metadata.get('durationMillis', 'Unknown')))

    def albumActions(self, album_id, action):
        """
        Perform actions on the album.
        :param album_id: The ID of the album to perform actions on.
        :param action: The action to perform (e.g., 'delete', 'share').
        """
        if action == 'delete':
            self.service.albums().delete(albumId=album_id).execute()
            logger.info("Album {} deleted.".format(album_id))
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
                    logger.warning("Album {} not found.".format(album_id))
                    return
                print("Album ID: {}".format(album.get('id')))
                print("Title: {}".format(album.get('title')))
                print("Description: {}".format(album.get('description', 'No description')))
                print("Product URL: {}".format(album.get('productUrl')))
                print("Writeable: {}".format(album.get('isWriteable')))
                print("Media items count: {}".format(album.get('mediaItemsCount', 0)))
                print("Cover photo base URL: {}".format(album.get('coverPhotoBaseUrl')))
                print("Cover photo media item ID: {}".format(album.get('coverPhotoMediaItemId')))
                print("Created time: {}".format(album.get('createdTime')))
                print("Updated time: {}".format(album.get('updatedTime')))
                print("Shareable: {}".format(album.get('shareable')))
                print("Shared album: {}".format(album.get('sharedAlbum')))
                print("Share token: {}".format(album.get('shareToken')))
                print("Shareable URL: {}".format(album.get('shareableUrl')))
                print("Owner: {} ({})".format(album.get('owner', {}).get('displayName', 'Unknown'), album.get('owner', {}).get('emailAddress', 'Unknown')))
            except Exception as e:
                logger.error("Error retrieving album info: {}".format(e))
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
            logger.warning("Unknown action: {}".format(action))

    def safe_batch_create(self, body, max_retries=5):
        for attempt in range(max_retries):
            try:
                return self.service.mediaItems().batchCreate(body=body).execute()
            except googleapiclient.errors.HttpError as e:
                if e.resp.status == 429:
                    wait = 2 ** attempt
                    logger.warning("Quota exceeded, retrying in {} seconds...".format(wait))
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
            logger.info("[Dry Run] Would upload large video {} to album {} with description '{}'".format(video_path, album_id, description))
            return

        headers = {
            'Authorization': "Bearer " + self.creds.token,
            'Content-Type': 'application/octet-stream',
            'X-Goog-Upload-File-Name': '"' + pathname2url(os.path.basename(video_path)) + '"',
            'X-Goog-Upload-Protocol': "raw",
        }
        upload_url = 'https://photoslibrary.googleapis.com/v1/uploads'
        try:
            logger.info("Uploading large video {} (streaming in chunks)".format(video_path))
            with open(video_path, "rb") as video_file:
                response = requests.post(upload_url, data=video_file, headers=headers, timeout=1800)
            if response.status_code != 200:
                logger.error("Failed to upload video {}: {} {}".format(video_path, response.status_code, response.text))
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
                logger.info("\tLarge video {} status {}".format(video_path.strip(self.sync_directory), media_result['newMediaItemResults'][0]['status']))
            else:
                logger.error("Error uploading large video {}: {}".format(video_path.strip(self.sync_directory), media_result))
        except Exception as err:
            logger.error("Error uploading large video {}\t{}".format(video_path.strip(self.sync_directory), err))
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
               print("{} - {}".format(title, album_id))
        sys.exit(0)
    if args.album_photos:
        album_id = args.album_photos
        if album_id.startswith('https://photos.app.goo.gl/'):
             album_id = album_id.split('/')[-1]
        logger.info("Album ID: {}".format(album_id))
        photo_sync.albumActions(album_id, 'photos')
        sys.exit(0)
    if args.album_info:
        album_id = args.album_info
        if album_id.startswith('https://photos.app.goo.gl/'):
            album_id = album_id.split('/')[-1]
        logger.info("Album ID: {}".format(album_id))
        photo_sync.albumActions(album_id, 'info')
        sys.exit(0)
    else:
        photo_sync.syncDirectory(args.directory if args.directory else None, force=args.force)
