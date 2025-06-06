import os
import hashlib
from datetime import datetime
from google_photos_auth import get_google_photos_credentials
from googleapiclient.discovery import build
from tqdm import tqdm
import sys
debug = False

if sys.version_info.major == 3 and sys.version_info.minor >= 10:
        import collections
        setattr(collections, "MutableMapping", collections.abc.MutableMapping)

def get_image_hash(image_path):
    """Compute SHA256 hash of the image file."""
    hash_sha256 = hashlib.sha256()
    with open(image_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def download_photos(target_root, dry_run=False):
    import requests
    SCOPES = [
        'https://www.googleapis.com/auth/photoslibrary.readonly',
        'https://www.googleapis.com/auth/photoslibrary.appendonly',
        'https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata'
    ]
    creds = get_google_photos_credentials(scopes=SCOPES)
    service = build('photoslibrary', 'v1', credentials=creds)

    # Get all albums
    albums = {}
    results = service.albums().list(pageSize=50, fields="nextPageToken,albums(id,title)").execute()
    items = results.get('albums', [])
    page_token = results.get('nextPageToken')
    while page_token is not None:
        results = service.albums().list(pageSize=50, fields="nextPageToken,albums(id,title)", pageToken=page_token).execute()
        items += results.get('albums', [])
        page_token = results.get('nextPageToken')
    for album in items:
        albums[album.get("id")] = album.get("title")

    # Download all media items
    next_page_token = None
    hashes = set()
    while True:
        body = {"pageSize": 100}
        if next_page_token:
            body["pageToken"] = next_page_token
        results = service.mediaItems().list(**body).execute()
        media_items = results.get('mediaItems', [])
        for item in tqdm(media_items, desc="Downloading photos"):
            filename = item.get('filename')
            base_url = item.get('baseUrl')
            media_metadata = item.get('mediaMetadata', {})
            creation_time = media_metadata.get('creationTime')
            if not creation_time:
                continue
            dt = datetime.fromisoformat(creation_time.replace('Z', '+00:00'))
            year = dt.strftime('%Y')
            month = dt.strftime('%m')
            day = dt.strftime('%d')
            # Download path
            year_dir = os.path.join(target_root, year)
            month_dir = os.path.join(year_dir, month)
            day_dir = os.path.join(month_dir, day)
            ensure_dir(day_dir)
            file_path = os.path.join(day_dir, filename)
            # Check for duplicates by hash
            if os.path.exists(file_path):
                if dry_run:
                    print(f"[Dry Run] Would check hash for {file_path}")
                    print(f"[Dry Run] {filename} already exists, skipping.")
                    continue
                import requests
                if get_image_hash(file_path) == hashlib.sha256(requests.get(base_url + "=d").content).hexdigest():
                    continue  # Already downloaded
                else:
                    # File exists but is different, skip or rename
                    file_path = os.path.join(day_dir, f"{os.path.splitext(filename)[0]}_dup{os.path.splitext(filename)[1]}")
            if dry_run:
                print(f"[Dry Run] Would download {filename} to {file_path}")
                continue
            # Download the image
            import requests
            r = requests.get(base_url + "=d")
            with open(file_path, 'wb') as f:
                f.write(r.content)
            # Set file's modification and access time to photo creation time
            try:
                ts = dt.timestamp()
                os.utime(file_path, (ts, ts))
            except Exception as e:
                print(f"Failed to set timestamp for {file_path}: {e}")
            # Add to album folder (symlink or copy) outside the year folder
            album_dir = os.path.join(target_root, 'album')
            ensure_dir(album_dir)
            album_link = os.path.join(album_dir, filename)
            if not os.path.exists(album_link):
                try:
                    os.symlink(file_path, album_link)
                except Exception:
                    pass  # On Windows, fallback to copy
    
        next_page_token = results.get('nextPageToken')
        if not next_page_token:
            break

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Download Google Photos hierarchically by year/month/day/image-name, avoid duplicates, and put in year album.")
    parser.add_argument('--target', type=str, default=os.path.expanduser('~/Pictures'), help='Target root directory')
    parser.add_argument('--dry-run', action='store_true', help='Dry run: only print actions, do not download')
    args = parser.parse_args()
    download_photos(args.target, dry_run=args.dry_run)
