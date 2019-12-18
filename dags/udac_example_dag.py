import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity'
    , 'start_date': datetime(2018, 11, 1)
    , 'depends_on_past': False
    , 'retries': 3
    , 'retry_delay': timedelta(minutes=5)
    , 'catchup_by_default': False
    , 'email_on_retry': False
}

dag = DAG(
          'udac_example_dag'
          , default_args=default_args
          , description='Load and transform data in Redshift with Airflow'
          , schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    , task_id='stage_events'
    , dag=dag
    , redshift_conn_id="redshift"
    , aws_credentials_id="aws_credentials"
    , table="staging_events"
    , s3_bucket="udacity-dend"
    , s3_key="log_data"
    , json_path="s3://udacity-dend/log_json_path.json"
    , file_type="json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs'
    , dag=dag
    , redshift_conn_id="redshift"
    , aws_credentials_id="aws_credentials"
    , table="staging_songs"
    , s3_bucket="udacity-dend"
    , s3_key="song_data/A/A/A"
    , json_path="auto"
    , file_type="json"
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table'
    , dag=dag
    , redshift_conn_id="redshift"
    , table='songplays'
    , sql_stmt=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    , task_id='load_user_dim_table'
    , dag=dag
    , redshift_conn_id="redshift"
    , table='users'
    , sql_stmt=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table'
    , dag=dag
    , redshift_conn_id="redshift"
    , table='songs'
    , sql_stmt=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table'
    , dag=dag
    , redshift_conn_id="redshift"
    , table='artists'
    , sql_stmt=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table'
    , dag=dag
    , redshift_conn_id="redshift"
    , table='time'
    , sql_stmt=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks'
    , dag=dag
    , redshift_conn_id="redshift"
    , tables=['songplays'
        , 'users'
        , 'songs'
        , 'artists'
        , 'time']
)

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)

# Start Operators
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# Stage Operators
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# Load songplay tables Operators
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# Load user, artist, time Operators
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# Quality/End Operators
run_quality_checks >> end_operator
