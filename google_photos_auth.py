import os
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
import google.auth.transport.requests

def get_google_photos_credentials(scopes=None, credentials_path=None, client_secret_path='client_secret.json'):
    if scopes is None:
        scopes = [
            'https://www.googleapis.com/auth/photoslibrary',
            'https://www.googleapis.com/auth/photoslibrary.appendonly',
            'https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata',
            'https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata'
        ]
    if credentials_path is None:
        credentials_path = os.path.expanduser('~/.PhotoSync/.credentials.json')
    credentials_directory = os.path.dirname(credentials_path)
    if not os.path.exists(credentials_directory):
        os.makedirs(credentials_directory)
    creds = None
    if os.path.exists(credentials_path):
        creds = Credentials.from_authorized_user_file(credentials_path, scopes)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(google.auth.transport.requests.Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, scopes)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(credentials_path, 'w') as token:
            token.write(creds.to_json())
    return creds
