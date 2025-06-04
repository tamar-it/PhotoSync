from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import os
import sys
from urllib.request import pathname2url

# Setup credentioals directory
credentials = '~/.PhotoSync/.credentials.json'
credentials_directory = os.path.dirname(credentials)
if not os.path.exists(credentials_directory):
    os.makedirs(credentials_directory)

# Setup the Photo v1 API
SCOPES = 'https://www.googleapis.com/auth/photoslibrary.readonly'

store = file.Storage(os.path.expanduser(credentials))
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)
    creds = tools.run_flow(flow, store)
service = build('photoslibrary', 'v1', http=creds.authorize(Http()))
sync_directory = os.path.expanduser("~/Pictures")

def uploadPhoto(album_id,photo_name,description=None):
		#batch = BatchHttpRequest()
		headers = {
			'Authorization': "Bearer " + service._http.request.credentials.access_token,
			'Content-Type': 'application/octet-stream',
			'X-Goog-Upload-File-Name': os.path.basename(photo_name),
			'X-Goog-Upload-Protocol': "raw",
		}
		with open(photo_name,"rb") as photo_file:
			media = photo_file.read()
		body = {"albumId":album_id,"newMediaItems":[{'description':description if description is not None else os.path.basename(photo_name)}]}

		try:
			print("uploading {}".format(photo_name))
			token=service._http.request('https://photoslibrary.googleapis.com/v1/uploads', method='POST', body=media, headers=headers)
		except client.NonAsciiHeaderError as err:
			headers['X-Goog-Upload-File-Name'] = pathname2url(headers['X-Goog-Upload-File-Name'])
			token=service._http.request('https://photoslibrary.googleapis.com/v1/uploads', method='POST', body=media, headers=headers)
		try:
			if token is not None:
				if album_id == "-":
					body.pop("albumId")
				body["newMediaItems"][0]["simpleMediaItem"] = {"uploadToken": token[1].decode('utf8')}
				media_result = service.mediaItems().batchCreate(body=body).execute()
				print("\tFile {} status {}".format(photo_name.strip(sync_directory), media_result['newMediaItemResults'][0]['status']))
		except Exception as err:
			print("Error adding media item {}: {}".format(description if description is not None else os.path.basename(photo_name),err))

if len(sys.argv)>2:
    uploadPhoto(sys.argv[1],sys.argv[2], sys.argv[3])
