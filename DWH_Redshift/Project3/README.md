# Project Data Warehouse from Logs

### Objective 

Sparkify the growing startup has needs of expanding the Data Warehousing capabilities.

The user, activity songs metadata data sits in json files in S3 buckets. The goal of the current project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables (Star Schema) such that the analytics team can continue to find insights on what songs are users are listening to.

## Steps (As suggested the project goals are achieved in below pieces)

1. Create Table Schema
2. Build ETL Pipeline
3. Document the process

### Create the AWS Cluster using IaC

1. Using below information from the configuration file written to dwh.cfg

[CLUSTER]
HOST=''
DB_NAME=''
DB_USER=''
DB_PASSWORD=''
DB_PORT=5439

[IAM_ROLE]
ARN=

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
KEY=
SECRET=

[DWH]
DWH_CLUSTER_TYPE       = multi-node
DWH_NUM_NODES          = 4
DWH_NODE_TYPE          = dc2.large
DWH_CLUSTER_IDENTIFIER = 
DWH_DB                 = 
DWH_DB_USER            = 
DWH_DB_PASSWORD        = 
DWH_PORT               = 5439
DWH_IAM_ROLE_NAME      = 

   a. Three scripts do the following tasks:
       aws_create.py -- Creates the cluster
       aws_check.py  -- Gives details of spun-up cluster asks to wait
       aws_delete.py -- Destroys the running/created cluster

2. New IAM role is created 
3. Existing policy is attached to the role
4. End points are collected for the cluster
5. Ports are opened on the Redshift cluster for VPC security group

### Create the structure of staging tables

1. Staging tables are created to dump the log data read from S3 buckets.

    a. By declaring table structure in sql_queries.py for staging_events, staging_events using IAM role created in the task above.

2. In Star Schema Facts & Dimension tables are created in sql_queries.py
3. Using Postgres SQL queries Insert queries are added to move data from staging tables to Star Schema


### Analytics/Check SQL queries are used to verify the data in DWH

1. Analytic queries are added to sql_queries.py for verification.
2. Order of execution is defined for droping, inserting data in tables

#### ETL script to copy, load the data

1. Finally etl.py is levereaged to copy data from S3 buckets to staging tables
2. Move data from staging tables to DWH
3. Run analytic/check queries defined in sql_queries.py

## Project Structure

In-total there are five essential script:

1. aws_create.py
2. create_tables.py
3. sql_queries.py
4. etl.py
5. README.md (the current file)

## Database Design Schema

### Staging Tables

staging_events
staging_songs

### Fact Table

songplay - records in event data associated with song plays i.e. records with page NextSong - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

users - users in the app - user_id, first_name, last_name, gender, level
songs - songs in music database - song_id, title, artist_id, year, duration
artists - artists in music database - artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units - start_time, hour, day, week, month, year, weekday

## ER diag for DWH table:
< 2_Final_Tables.png >



## Result from Analytic Queries on DW

    SELECT COUNT(*) FROM staging_events
 -- 
  16112

    SELECT COUNT(*) FROM staging_songs
 -- 
  29792
 
    SELECT COUNT(*) FROM songplay
 -- 
  2256

    SELECT COUNT(*) FROM users
 -- 
  208
 
    SELECT COUNT(*) FROM songs
 -- 
  29792

    SELECT COUNT(*) FROM artists
 -- 
  20050

    SELECT COUNT(*) FROM time
 -- 
  16046



