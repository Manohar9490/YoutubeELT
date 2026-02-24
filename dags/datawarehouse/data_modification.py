import logging

logger = logging.getLogger(__name__)
table_name = "video_stats"

def insert_row(cur, conn,schema, row):

  try:
    if schema == 'staging':

      video_id = 'videoId'

      insert_query = f"""
      INSERT INTO {schema}.{table_name} (video_id, video_title, upload_date, duration, video_views, likes_count, comments_count)
      VALUES (%(videoId)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)
      ON CONFLICT (video_id) DO NOTHING;
      """
      cur.execute(insert_query, row)
      conn.commit()
    else:
      video_id = 'video_id'
      insert_query = f"""
      INSERT INTO {schema}.{table_name} (video_id, video_title, upload_date, duration, video_views, likes_count, comments_count)
      VALUES (%(video_id)s, %(video_title)s, %(upload_date)s, %(duration)s, %(video_views)s, %(likes_count)s, %(comments_count)s)
      ON CONFLICT (video_id) DO NOTHING;
      """
      cur.execute(insert_query, row)
      conn.commit()
    
    logger.info(f"Inserted row for video_id: {row[video_id]}")
  except Exception as e:
    logger.error(f"Error inserting row: {row[video_id]}")
    raise e
  
def update_row(cur, conn, schema, row):
  try:
    if schema == 'staging':
      video_id = 'videoId'
      update_query = f"""
      UPDATE {schema}.{table_name}
      SET video_title = %(title)s,
          upload_date = %(publishedAt)s,
          duration = %(duration)s,
          video_views = %(viewCount)s,
          likes_count = %(likeCount)s,
          comments_count = %(commentCount)s
      WHERE video_id = %(videoId)s;
      """
      cur.execute(update_query, row)
      conn.commit()
    else:
      video_id = 'video_id'
      update_query = f"""
      UPDATE {schema}.{table_name}
      SET video_title = %(video_title)s,
          upload_date = %(upload_date)s,
          duration = %(duration)s,
          video_views = %(video_views)s,
          likes_count = %(likes_count)s,
          comments_count = %(comments_count)s
      WHERE video_id = %(video_id)s;
      """
      cur.execute(update_query, row)
      conn.commit()
    
    logger.info(f"Updated row for video_id: {row[video_id]}")
  except Exception as e:
    logger.error(f"Error updating row: {row[video_id]}")
    raise e
  
def delete_row(cur, conn, schema, ids_to_delete):
  try:
    ids_to_delete = f"""({','.join(f'{video_id}' for video_id in ids_to_delete)})"""

    cur.execute(f"DELETE FROM {schema}.{table_name} WHERE video_id IN {ids_to_delete};")
    conn.commit()
    logger.info(f"Deleted rows for video_ids: {ids_to_delete}")
  except Exception as e:
    logger.error(f"Error deleting rows for video_ids: {ids_to_delete}")
    raise e
