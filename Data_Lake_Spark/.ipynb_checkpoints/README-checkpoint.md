# Project 4 - Data Lake

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project we will build an ETL pipeline that extracts their data from the data lake hosted on S3, processes them using Spark which will be deployed on an EMR cluster using AWS, and load the data back into S3 as a set of dimensional tables in parquet format.

From this tables we will be able to find insights in what songs their users are listening to.

## Deployment

File dl.cfg is not provided here. File contains :

'KEY=YOUR_AWS_ACCESS_KEY'
'SECRET=YOUR_AWS_SECRET_KEY'


Create an S3 Bucket named bucket-sachaan where output results will be stored.

Finally, run the following command:

python etl.py

To run on an Jupyter Notebook powered by an EMR cluster, import the notebook found in this project.

## ETL Pipeline

1. Read data from S3

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data

The script reads song_data and load_data from S3.

2. Process data using spark

Transforms them to create five different tables listed below :

- Fact Table

songplays - records in log data associated with song plays i.e. records with page NextSong

songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- Dimension Tables

users - users in the app Fields - user_id, first_name, last_name, gender, level

songs - songs in music database Fields - song_id, title, artist_id, year, duration

artists - artists in music database Fields - artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units Fields - start_time, hour, day, week, month, year, weekday

3. Load it back to S3

Writes them to partitioned parquet files in table directories on S3.