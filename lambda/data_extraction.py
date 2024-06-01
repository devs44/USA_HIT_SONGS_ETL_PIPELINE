import json
import spotipy
import boto3
import os

from datetime import datetime
from spotipy.oauth2 import SpotifyClientCredentials

def lambda_handler(event, context):
    client_id = os.environ.get('CLIENT_ID')
    client_secret = os.environ.get('CLIENT_SECRET')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    sp = spotipy.Spotify(auth_manager=client_credentials_manager)
    playlists = sp.user_playlists('spotify')
    
    playlist_link = 'https://open.spotify.com/playlist/37i9dQZF1DX0kbJZpiYdZl'
    
    client = boto3.client('s3')
    
    def get_data():
        playlist_uri = playlist_link.split("/")[-1]
        data = sp.playlist_tracks(playlist_uri)
        return data 
        
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
        
    client.put_object(
            Bucket = "spotify-airflow-project-devi",
            Key = "raw_data/to_processed/" + filename,
            Body = json.dumps(get_data())
        )
            
