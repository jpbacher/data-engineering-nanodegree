# Cloud Data Warehouse Project

## Description
Sparkify, a hypothetical music streaming startup, has grown their user base and song database and would like to move their processes
and data onto the cloud. Their data, user activity and song metadata, resides in json files in S3.

The project's task is to build an ETL pipeline that extracts data from S3, stages the data in Redshift, and transforms
the data into a set of dimensional tables for the analytics team to further gain insights on their users' behaviors.

## Project Construction
* *build_cluster.py*: the AWS components created programmatically 
* *create_tables.py*: the fact and dimensional tables created (using star schema) in Redshift
* *dwh.cfg*: information for AWS, the Redshift cluster, DWH, and S3
* _etl.py_: from S3, data gets loaded into staging tables on Redshift and is processed into analytics tables
* *logic.py*: runs a few queries to validate the project
* *queries.py*: SQL statement definitions, used in *create_tables.py*, *etl.py*, and *logic.py* scripts

## Schema Design
**Staging Tables**
* staging_events
* staging_songs

**Fact Table**
* songplays - instances in event data with page=='NextSong': *songplay_id*, *start_time*, *user_id*, *level*, *song_id*, *artist_id*, 
*session_id*, *location*, *user_agent*

**Dimensional Tables**
* users - users: *user_id*, *first_name*, *last_name*, *gender*, *level*
* songs - songs residing in music DB: *song_id*, *title*, *artist_id*, *year*, *duration*
* artist - artists in music DB: *artist_id*, *name*, *location*, *lattitude*, *longitude*
* time - timestamps of instances in **songplays** table: *start_time*, *hour*, *day*, *week*, *month*, *year*, *weekday*

## Installation 
1. Fill in the information in the *dwh.cfg* file 
2. Run *build_cluster* file to set up foundation:
'''python
python build_cluster.py
'''
3. Run *create_tables* to establish DB staging and analytical tables:
'''python
python create_tables.py
'''
4. Run *etl* to acquire data from files in S3, stage the data in Redshift, and store in dimensional tables:
'''python
python etl.py
'''

## Final Output
