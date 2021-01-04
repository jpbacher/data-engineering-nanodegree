## Data Modeling with Postgres

### Introduction
Sparkify, a recent startup, would like to analyze the data they have been acquiring on their users' activity and songs, 
via their streaming app. Sparkify is interested in understanding what songs users are currently listening to.Currently, 
they do not have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, 
as well as a directory with JSON metadata on the songs in their app.

Therefore, we need to create a postgres database with tables designed to optimize queries on song play analysis, and 
create a schema and ETL pipeline for this analysis.

### Database Schema
We will use a star schema for this problem: there exists one fact table containing the measurables associated with each
even - user song plays - and four dimensional tables. Each dimension table has a primary key referenced from the fact table.

We will use a relational database for this problem:
* the data types are structured - we know the structure of the jsons
* the data is relatively small - we do not need big data related solutions
* need to use JOINs for this use case