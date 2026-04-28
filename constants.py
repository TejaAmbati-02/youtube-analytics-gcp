import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), "config/config.local"))

YOUTUBE_API_KEY = parser.get("youtube","yt_api_key")
PLAYLIST_ID = parser.get("youtube","playlist_id")
