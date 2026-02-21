import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

YOUR_API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"
maxResults = 50

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


def get_video_ids(playlist_id):
    
    video_ids = []
    pageToken = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlist_id}&key={YOUR_API_KEY}"
    try:
       while True:
          url = base_url
          if pageToken:
              url += f"&pageToken={pageToken}"
          
          response = requests.get(url)
          response.raise_for_status()  # Check if the request was successful

          data = response.json()

          for item in data.get('items', []):
              video_ids.append(item['contentDetails']['videoId'])

          pageToken = data.get('nextPageToken')
          if not pageToken:
              break

          return video_ids
    except requests.exceptions.RequestException as e:
      raise e



def get_video_stats(video_ids):
    video_stats = []

    def batch_list(lst, batch_size):
      for i in range(0, len(lst), batch_size):
          yield lst[i:i + batch_size]

    base_url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&key={YOUR_API_KEY}"

    try:
       for batch in batch_list(video_ids,maxResults):
          
          video_id_string = ",".join(batch)

          url = f"{base_url}&id={video_id_string}"

          response = requests.get(url)

          response.raise_for_status()  # Check if the request was successful

          data = response.json()

          for item in data.get('items', []):
              video_stats.append({
                  'videoId': item['id'],
                  'title': item['snippet']['title'],
                  'publishedAt': item['snippet']['publishedAt'],
                  'duration': item['contentDetails']['duration'],
                  'viewCount': item['statistics'].get('viewCount', None),
                  'likeCount': item['statistics'].get('likeCount', None),
                  'commentCount': item['statistics'].get('commentCount', None)
              })
          return video_stats   
    except requests.exceptions.RequestException as e:
      raise e


if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_stats = get_video_stats(video_ids)
    # print(video_ids)
    print(video_stats)