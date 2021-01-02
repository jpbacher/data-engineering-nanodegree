# Cloud Data Warehouse Project

## Description
Sparkify, a hypothetical musing streaming startup, has grown their user base and song database and would like to move their processes
and data onto the cloud. Their data, user activity and song metadata, resides in json files in S3.

The project's task is to build an ETL pipeline that extracts data from S3, stages the data in Redshift, and transforms
the data into a set of dimensional tables for the analytics team to further gain insights on their users' behaviors.

## Project Construction
* *build_cluster.py*: the AWS components created programmatically 
* *create_tables.py*: the fact and dimensional tables created (using star schema) in Redshift
* *dwh.cfg*: information for AWS, the Redshift cluster, S3
* _etl.py_: from S3, data gets loaded into staging tables on Redshift and is processed into analytics tables
* *logic.py*: runs a few queries to validate the project
* *queries.py*: SQL statement definitions, used in *create_tables.py*, *etl.py*, and *logic.py* scripts 