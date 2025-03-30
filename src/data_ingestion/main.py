import pandas as pd
from utils.file_io import InputOutput as IO
from utils.keys import youtube_api_key
from utils.youtube_client import YoutubeClient
import json

youtube = YoutubeClient(youtube_api_key)

response = youtube.search('snippet','monster energy drink',50)

print(json.dumps(response,indent=2))