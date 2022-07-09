from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
s3_bucket = "udacity-dend"
log_s3_key = "log_data"
song_s3_key = "song_data"
json_path = "json_paths.json"

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': True,
    'email_on_retry': False,  # set to True to get email when retry
    'email_on_failure': False  # set to True to get email when failure
}

dag = DAG('etl_s3_to_redshift_dag',
        default_args=default_args,
        description='Load and transform data in Redshift with Airflow',
        schedule_interval='@daily',
        max_active_runs=1
        )

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

create_tables_in_redshift = PostgresOperator(
    task_id='Create_tables_in_redshift',
    postgres_conn_id='redshift',
    sql='create_tables.sql',
    dag=dag
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table_name='public.staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket=s3_bucket,
    s3_key=log_s3_key,
    json_path=json_path,
    file_format='json',
    provide_context=True,
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table_name='public.staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket=s3_bucket,
    s3_key=song_s3_key,
    file_format='json',
    provide_context=True,
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    table_name='public.songplays',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    table_name='public.users',
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.songplay_table_insert,
    append_only=False,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    table_name='public.songs',
    load_sql_query=SqlQueries.song_table_insert,
    redshift_conn_id='redshift',
    append_only=False,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    table_name='public.artists',
    load_sql_query=SqlQueries.artist_table_insert,
    redshift_conn_id='redshift',
    append_only=False,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    table_name='public.time',
    load_sql_query=SqlQueries.time_table_insert,
    redshift_conn_id='redshift',
    append_only=False,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    tables=['public.songplays', 'public.users', 'public.songs', 'public.artists', 'public.time'],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_in_redshift >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator



