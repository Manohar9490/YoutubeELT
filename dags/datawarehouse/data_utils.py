from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyscopg2.extras import realDictCursor

table = 'yt_api'

def get_conn_cursor():
    pg_hook = PostgresHook(postgres_conn_id='AIRFLOW_CONN_POSTGRES_DB_YT_ELT',database='elt_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor(cursor_factory=realDictCursor)
    return conn, cursor

def close_conn_cursor(conn, cursor):
    cursor.close()
    conn.close()


def create_schema(schema):
    conn, cursor = get_conn_cursor()
    create_schema_query = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};
    """
    cursor.execute(create_schema_query)
    conn.commit()
    close_conn_cursor(conn, cursor)

def create_table(schema, table):
    conn, cursor = get_conn_cursor()

    if schema == 'staging':
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            'video_id' VARCHAR(11) PRIMARY KEY NOT NULL,
            'video_title' TEXT NOT NULL,
            'Upload_date' TIMESTAMP NOT NULL,
            'duration' VARCHAR(20) NOT NULL,
            'video_views' INT,
            'likes_count' INT,
            'comments_count' INT
        );
        """

    else:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            'video_id' VARCHAR(11) PRIMARY KEY NOT NULL,
            'video_title' TEXT NOT NULL,
            'Upload_date' TIMESTAMP NOT NULL,
            'duration' VARCHAR(20) NOT NULL,
            'video_views' INT,
            'likes_count' INT,
            'comments_count' INT
        );
        """
        
    cursor.execute(create_table_query)
    conn.commit()
    close_conn_cursor(conn, cursor)

def get_video_ids(cur, schema, table):
    query = f"SELECT video_id FROM {schema}.{table};"
    cur.execute(query)
    video_ids = [row['video_id'] for row in cur.fetchall()]
    return video_ids