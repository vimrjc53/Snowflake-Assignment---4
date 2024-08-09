from urllib import response
import boto3
import requests
import json


#List of streamers to get access to their stats

list = requests.get("https://api.chess.com/pub/streamers").json()
list_username = []
number_streamer = len(list['streamers'])
#creation of a list of all the streamers username
for i in range(number_streamer):
    list_username.append(list['streamers'][i]['username'])

info = []
stats = []

#retrieving information and games statistics for each streamer

for i in range(number_streamer):
    info.append(requests.get("https://api.chess.com/pub/player/" + list_username[i]).json())
    stats.append(requests.get("https://api.chess.com/pub/player/" + list_username[i] + "/stats").json())
    print(i)


#injecting into S3 bucket

s3 = boto3.resource('s3',
    aws_access_key_id='<aws_access_key>',
    aws_secret_access_key= '<aws_secret_access_key>')

object1 = s3.Object('my-chess-bucket', 'list')
object1.put(Body=json.dumps(list))

object1 = s3.Object('my-chess-bucket', 'info')
object1.put(Body=json.dumps(info))

object1 = s3.Object('my-chess-bucket', 'stats')
object1.put(Body=json.dumps(stats))
