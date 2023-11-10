from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
#from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
#from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'on_retry_callback': True,
    'on_failure_callback': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = EmptyOperator(task_id='Begin_execution')

    create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id="redshift",
    sql='create_redshift_tables.sql',
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        aws_credentials_id = 'aws_credentials',
        redshift_conn_id = 'redshift',
        table = 'staging_events',
        s3_bucket = 'akwayaga',
        s3_key = 'log-data',
        region = 'us-west-2'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        aws_credentials_id = 'aws_credentials',
        redshift_conn_id = 'redshift',
        table = 'staging_songs',
        s3_bucket = 'akwayaga',
        s3_key = 'song-data',
        region = 'us-west-2'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = 'redshift',
        table = 'songplays',
        sql = SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id = 'redshift',
        table = 'users',
        sql = SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = 'redshift',
        table = 'songs',
        sql = SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = 'redshift',
        table = 'artists',
        sql = SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = 'redshift',
        table = 'time',
        sql = SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = 'redshift',
        table = 'songplays'  
    )

    end_operator = EmptyOperator(task_id='End_execution')

    # set task dependencies
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    start_operator >> create_tables
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table << create_tables
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()