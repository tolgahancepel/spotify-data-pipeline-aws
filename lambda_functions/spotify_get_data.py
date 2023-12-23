"""
File: spotify_get_data.py
Author: Tolgahan Cepel
Email: tolgahan.cepel@gmail.com
Date: December 20, 2023
Description: AWS Lambda function for processing API data to store in S3 bucket.
"""

import os
import json
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    client_credential_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    
    sp = spotipy.Spotify(client_credentials_manager=client_credential_manager)
    
    playlist_link = 'https://open.spotify.com/playlist/37i9dQZEVXbIVYVBNw9D5K'
    playlist_id = playlist_link.split('/')[-1]
    
    data = sp.playlist_tracks(playlist_id)
    
    filename = datetime.now().strftime("%Y%m%d") + '_spotify_raw_data' + '.json'
    
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='spotify-etl-pipeline-cepel',
        Key = 'raw_data/' + filename,
        Body = json.dumps(data)
    )
    
    return {
        'statusCode': 200,
        'body': filename + ' stored in S3 successfully!'
    }
