import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

YOUR_API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"

# json.dumps(data, indent=4)
# print(json.dumps(data, indent=4))

## To write the data to a JSON file
# with open('Video_stats.json', 'a') as f:
#     # json.dump(data, f, indent=4)
#     json_string = json.dumps(data, indent=4)
#     f.write(json_string + "\n")
# print(response)

def get_playlist_id():
  try:
    url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={YOUR_API_KEY}'

    response = requests.get(url)

    response.raise_for_status()  # Check if the request was successful

    data = response.json()

    Channel_items = data['items'][0]

    Channel_playlist_id = Channel_items['contentDetails']['relatedPlaylists']['uploads']

    return Channel_playlist_id
  
  except requests.exceptions.RequestException as e:
    raise e


if __name__ == "__main__":
    playlist_id = get_playlist_id()
    print(playlist_id)
