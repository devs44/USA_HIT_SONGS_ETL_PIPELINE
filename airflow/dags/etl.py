from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeTransferOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'spotify_etl_pipeline',
    default_args=default_args,
    description='An ETL pipeline for Spotify data using Airflow, Lambda, and Snowflake',
    schedule_interval=timedelta(days=1),
)

# Define Python function to invoke Lambda functions
def invoke_lambda_function(function_name, payload={}):
    hook = AwsLambdaHook(aws_conn_id='aws_default', function_name=function_name, invocation_type='Event')
    response = hook.invoke_lambda(payload=payload)
    return response

# Task to invoke the data extraction Lambda function
extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=invoke_lambda_function,
    op_kwargs={'function_name': 'data_extraction'},
    dag=dag,
)

# Task to invoke the data loading Lambda function
transform_load_data = PythonOperator(
    task_id='transform_load_data',
    python_callable=invoke_lambda_function,
    op_kwargs={'function_name': 'data_loading'},
    dag=dag,
)

# Task to load album data into Snowflake
load_album_into_snowflake = S3ToSnowflakeTransferOperator(
    task_id='load_album_into_snowflake',
    snowflake_conn_id='snowflake_default',
    s3_keys=['transformed_data/album_data/album_transformed_*.csv'],
    stage='@SPOTIFY_AIRFLOW.EXTERNAL_STAGES.ALBUM_EXT_STAGE',
    table='tblalbum',
    schema='SPOTIFY_AIRFLOW_SCHEMA',
    file_format='(type = csv field_delimiter = \',\' skip_header = 1)',
    dag=dag,
)

# Task to load artist data into Snowflake
load_artist_into_snowflake = S3ToSnowflakeTransferOperator(
    task_id='load_artist_into_snowflake',
    snowflake_conn_id='snowflake_default',
    s3_keys=['transformed_data/artist_data/artist_transformed_*.csv'],
    stage='@SPOTIFY_AIRFLOW.EXTERNAL_STAGES.ARTIST_EXT_STAGE',
    table='tblArtist',
    schema='SPOTIFY_AIRFLOW_SCHEMA',
    file_format='(type = csv field_delimiter = \',\' skip_header = 1)',
    dag=dag,
)

# Task to load song data into Snowflake
load_song_into_snowflake = S3ToSnowflakeTransferOperator(
    task_id='load_song_into_snowflake',
    snowflake_conn_id='snowflake_default',
    s3_keys=['transformed_data/songs_data/songs_transformed_*.csv'],
    stage='@SPOTIFY_AIRFLOW.EXTERNAL_STAGES.SONG_EXT_STAGE',
    table='tblSongs',
    schema='SPOTIFY_AIRFLOW_SCHEMA',
    file_format='(type = csv field_delimiter = \',\' skip_header = 1)',
    dag=dag,
)

# Define task dependencies
extract_data >> transform_load_data >> [load_album_into_snowflake, load_artist_into_snowflake, load_song_into_snowflake]
