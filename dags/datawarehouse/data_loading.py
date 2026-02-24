import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_path():
  file_path = f"./data/video_stats_{date.today()}.json"

  try:
    logger.info(f"Processing file: video_stats_{date.today()}")

    with open(file_path, 'r', encoding='utf-8') as f:
        video_stats = json.load(f)
    return video_stats
  except FileNotFoundError:
    logger.error(f"File not found: {file_path}")
    raise