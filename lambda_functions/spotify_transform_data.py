"""
File: spotify_transform_data.py
Author: Tolgahan Cepel
Email: tolgahan.cepel@gmail.com
Date: December 20, 2023
Description: AWS Lambda function for processing raw data and storing back into S3 bucket.
"""

import pandas as pd
import boto3
from datetime import datetime
import json
from io import StringIO


def extract_albums(json_data):
    albums_list = []
    for row in json_data["items"]:
        album_dict = {
            "album_id": row["track"]["album"]["id"],
            "album_name": row["track"]["album"]["name"],
            "release_date": row["track"]["album"]["release_date"],
            "total_tracks": row["track"]["album"]["total_tracks"],
            "spotify_url": row["track"]["album"]["external_urls"]["spotify"],
        }
        albums_list.append(album_dict)
    return albums_list

def extract_artists(json_data):
    artists_list = []
    for row in json_data["items"]:
        for artist in row["track"]["album"]["artists"]:
            album_dict = {
                "artist_id": artist["id"],
                "artist_name": artist["name"],
                "artist_type": artist["type"],
                "spotify_url": artist["external_urls"]["spotify"],
            }
        artists_list.append(album_dict)
    return artists_list


def extract_songs(json_data):
    songs_list = []
    for row in json_data["items"]:
        song_dict = {
            "song_id": row["track"]["id"],
            "album_id": row["track"]["album"]["id"],
            "song_name": row["track"]["name"],
            "duration_ms": row["track"]["duration_ms"],
            "added_at": row["added_at"],
            "popularity": row["track"]["popularity"],
            "spotify_url": row["track"]["external_urls"]["spotify"]
        }
        songs_list.append(song_dict)
    return songs_list


def lambda_handler(event, context):

    client = boto3.client("s3")
    Bucket = "spotify-etl-pipeline-cepel"
    Key = 'raw_data/'

    spotify_data = []
    spotify_keys = []

    for file in client.list_objects(Bucket=Bucket, Prefix=Key)["Contents"]:
        file_key = file['Key']
        
        if file_key.split('.')[-1] == "json":
            response = client.get_object(Bucket=Bucket, Key=file_key)
            content = response["Body"]
            jsonObject = json.loads(content.read())
            
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)

    for data in spotify_data:
        album_list = extract_albums(data)
        artist_list = extract_artists(data)
        song_list = extract_songs(data)

        # Converting each list into a dataframe
        album_df = pd.DataFrame(album_list)
        artist_df = pd.DataFrame(artist_list)
        song_df = pd.DataFrame(song_list)

        # Droping possible duplicates
        album_df = album_df.drop_duplicates(subset=['album_id'])
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        song_df = song_df.drop_duplicates(subset=['song_id'])
        
        album_df["album_release_date"] = pd.to_datetime(album_df["release_date"], format='%Y-%m-%d')
        song_df['song_added_at'] = pd.to_datetime(album_df["release_date"], format='%Y-%m-%d')
    
        # Converting each dataframe into a csv file
        # Album
        album_key = "album_data/album_transformed_" +  datetime.now().strftime("%Y%m%d") + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()

        # Artist
        artist_key = "artist_data/artist_transformed_" +  datetime.now().strftime("%Y%m%d") + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()

        # Song
        song_key = "song_data/song_transformed_" +  datetime.now().strftime("%Y%m%d") + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()

        client.put_object(
            Bucket='spotify-etl-pipeline-cepel',
            Key = 'processing/' + album_key,
            Body = album_content
        )

        client.put_object(
            Bucket='spotify-etl-pipeline-cepel',
            Key = 'processing/' + artist_key,
            Body = artist_content
        )

        client.put_object(
            Bucket='spotify-etl-pipeline-cepel',
            Key = 'processing/' + song_key,
            Body = song_content
        )

    return {
        'statusCode': 200,
        'body': ' Transformed data stored in S3 successfully!'
    }
