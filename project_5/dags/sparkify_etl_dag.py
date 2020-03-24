from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

"""
Define DAG for Sparkify ETL Pipeline

Pipeline will read data from S3 bucket and load into Redshift Start schema based tables
"""

default_args = {
    'owner': 'charlehl',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False  
}

append_data_flag = False

dag = DAG('sparkify_s3_redshift_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

# Use to create tables if necessary
start_operator = PostgresOperator(
    task_id="Begin_execution",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

# Load staging table from S3 bucket
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = "staging_events",
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json",
    append_data = append_data_flag,
    provide_context=True
)
# Load staging table from S3 bucket                    
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "staging_songs",
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    append_data = append_data_flag,
    s3_bucket="udacity-dend",
    s3_key="song_data"
)
# Load fact table from staging tables
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    fact_table = "songplays",
    redshift_conn_id = "redshift",
    append_data = append_data_flag,
    sql_statement=SqlQueries.songplay_table_insert
)
# Load dimension table from staging tables
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    dim_table = "users",
    redshift_conn_id = "redshift",
    append_data = append_data_flag,
    sql_statement=SqlQueries.user_table_insert
)
# Load dimension table from staging tables
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    dim_table = "songs",
    redshift_conn_id = "redshift",
    append_data = append_data_flag,
    sql_statement=SqlQueries.song_table_insert
)
# Load dimension table from staging tables
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    dim_table = "artists",
    redshift_conn_id = "redshift",
    append_data = append_data_flag,
    sql_statement=SqlQueries.artist_table_insert
)
# Load dimension table from staging tables
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    dim_table = "times",
    redshift_conn_id = "redshift",
    append_data = append_data_flag,
    sql_statement=SqlQueries.time_table_insert
)

# Data quality checks for ETL pipeline
dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM times WHERE start_time is NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songplays", 'check_sql2': "SELECT COUNT(*) FROM staging_events WHERE page='NextSong'"}
    ]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    dq_checks = dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Stage 0
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
# Stage 1
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
# Stage 2
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
# Stage 3
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
# Stage 4
run_quality_checks >> end_operator

