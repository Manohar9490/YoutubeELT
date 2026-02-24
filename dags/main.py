from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, get_video_stats, save_to_json

# Define the local timezone
local_tz = pendulum.timezone("America/New_York")

# Define Args
default_args = {
    'owner': 'DE',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'emails': 'manohar@dataengineer.com',
    # 'retries': 1,
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(minutes=60),
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1, tzinfo=local_tz),
}

with DAG(
  dag_id='produce_json',
  default_args=default_args,
  description='A DAG to extract video stats from YouTube API and save it to a JSON file',
  schedule_interval='0 14 * * *',  # Schedule to run at 2 PM every day
  catchup=False
) as dag:

    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_stats = get_video_stats(video_ids)
    save_to_json_task= save_to_json(video_stats)

    #define task dependencies
    playlist_id >> video_ids >> video_stats >> save_to_json_task