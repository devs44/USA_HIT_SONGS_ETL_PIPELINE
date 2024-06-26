CREATE OR REPLACE DATABASE SPOTIFY_AIRFLOW;

CREATE OR REPLACE SCHEMA SPOTIFY_AIRFLOW_SCHEMA;

CREATE OR REPLACE SCHEMA FILE_FORMATS;

CREATE OR REPLACE SCHEMA EXTERNAL_STAGES;

--table creation
CREATE OR REPLACE TABLE tblAlbum(
    album_id STRING,
    name STRING,
    release_date STRING,
    total_tracks INTEGER,
    url STRING
);

CREATE OR REPLACE TABLE SPOTIFY_AIRFLOW.SPOTIFY_AIRFLOW_SCHEMA.tblArtist(
    artist_id STRING,
    artist_name STRING,
    external_url STRING
);

CREATE OR REPLACE TABLE SPOTIFY_AIRFLOW.SPOTIFY_AIRFLOW_SCHEMA.tblSongs(
    song_id STRING,
    song_name STRING,
    duration_ms INTEGER,
    url STRING,
    popularity INTEGER,
    song_added TIMESTAMP,
    album_id STRING,
    artist_id STRING
);

--storage integration
CREATE OR REPLACE STORAGE INTEGRATION ALBUM_S3_INIT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::798585594565:role/snowflake-s3-connection'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-airflow-project-devi/transformed_data/')
    COMMENT = 'CREATE CONNECTION TO S3';


DESC INTEGRATION ALBUM_S3_INIT;

--FILE FORMAT


CREATE OR REPLACE FILE FORMAT SPOTIFY_AIRFLOW.FILE_FORMATS.CSV
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1 -- Adjust based on your file structure
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('\\N', 'NULL', '');


--stage creation

CREATE OR REPLACE STAGE SPOTIFY_AIRFLOW.EXTERNAL_STAGES.ALBUM_EXT_STAGE
URL = 's3://spotify-airflow-project-devi/transformed_data/album_data/'
STORAGE_INTEGRATION=ALBUM_S3_INIT
FILE_FORMAT = SPOTIFY_AIRFLOW.FILE_FORMATS.CSV;

CREATE OR REPLACE STAGE SPOTIFY_AIRFLOW.EXTERNAL_STAGES.ARTIST_EXT_STAGE
URL = 's3://spotify-airflow-project-devi/transformed_data/artist_data/'
STORAGE_INTEGRATION=ALBUM_S3_INIT
FILE_FORMAT = SPOTIFY_AIRFLOW.FILE_FORMATS.CSV;

CREATE OR REPLACE STAGE SPOTIFY_AIRFLOW.EXTERNAL_STAGES.SONG_EXT_STAGE
URL = 's3://spotify-airflow-project-devi/transformed_data/songs_data/'
STORAGE_INTEGRATION=ALBUM_S3_INIT
FILE_FORMAT = SPOTIFY_AIRFLOW.FILE_FORMATS.CSV;

--LOAD DATA USING COPY COMMAND

COPY INTO SPOTIFY_AIRFLOW.SPOTIFY_AIRFLOW_SCHEMA.tblalbum
FROM @SPOTIFY_AIRFLOW.EXTERNAL_STAGES.ALBUM_EXT_STAGE
FILE_FORMAT = SPOTIFY_AIRFLOW.FILE_FORMATS.CSV;

COPY INTO SPOTIFY_AIRFLOW.SPOTIFY_AIRFLOW_SCHEMA.tblArtist
FROM @SPOTIFY_AIRFLOW.EXTERNAL_STAGES.ARTIST_EXT_STAGE
FILE_FORMAT = SPOTIFY_AIRFLOW.FILE_FORMATS.CSV;

COPY INTO SPOTIFY_AIRFLOW.SPOTIFY_AIRFLOW_SCHEMA.tblSongs
FROM @SPOTIFY_AIRFLOW.EXTERNAL_STAGES.SONG_EXT_STAGE
FILE_FORMAT = SPOTIFY_AIRFLOW.FILE_FORMATS.CSV;

SELECT * FROM SPOTIFY_AIRFLOW.SPOTIFY_AIRFLOW_SCHEMA.tblalbum

SELECT * FROM SPOTIFY_AIRFLOW.SPOTIFY_AIRFLOW_SCHEMA.tblArtist

SELECT * FROM SPOTIFY_AIRFLOW.SPOTIFY_AIRFLOW_SCHEMA.tblSongs