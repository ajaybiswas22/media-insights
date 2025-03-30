import pandas as pd
from utils.youtube_client import YoutubeClient
import json

youtube = YoutubeClient()

response = youtube.search('snippet','monster energy drink',50)

print(json.dumps(response,indent=2))