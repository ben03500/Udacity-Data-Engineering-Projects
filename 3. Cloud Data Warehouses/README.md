# Project: Data Warehouse with Redshift

This is my third project as part of the Udacity Data Engineering Nanodegree course. This Sparkify Redshift database contains data from song and users log data in order to help analytical process in our business needs. This database ensure that the data is properly organized, integral and can be easily access with queries.

## Project Template

1. ```create_table.py``` performs operations to create staging and dimensional tables
2. ```etl.py``` perform ETL process from staging tables, transform the data and load them into dimensional model tables.
3. ```sql_queries.py``` contains SQL queries for use in the first 2 files.


## Database schema design

There are 2 tables in the database which are use as the staging area:

1. ```staging_events```
2. ```staging_songs```

The ```staging_events``` is use for saving raw log data. And ```staging_songs``` is use for saving raw song related data from S3.

This analytic database contains 5 following tables:

1. ```songplays```
2. ```users```
3. ```songs```
4. ```artists```
5. ```time```

This design is similar to what call "Star-schema", where the `songplays` is the fact table that contain 'fact' about when customers played a song. And the rest of the tables are dimension tables that contains additional information that referred in fact table. i.e., `users` table contains supplement details about each user.

This schema denormalized the data a bit, but it help simplify query and improve query performance.

## How to run the project

First, setup a Redshift cluster in a security group that allows inbound traffic on the port 5439 and associate with role that has `AmazonS3ReadOnlyAccess` policy attached. Then, fill out necessary Redshift cluster information in `dwh.cfg` 

After that run this command to create table in the database:

```bash
$ python3 create_tables.py
```

This ETL pipeline consists of 3 steps: (1) Extraction: the song and users log data is extracted from JSON files into the staging tables. (2) Transform data into appropriate format. And (3) Load: The transformed data is loaded onto the database.

```bash
$ python3 etl.py
```

To update this database; the log data might be collect and perform ETL on a daily basis. And the song data might also be collect every month or when artist release a new song.

## Sample SQL queries

Find most popular artist on Sparkify

```sql
SELECT artists.name, count(*) as popularity FROM songplays JOIN artists USING (artist_id) GROUP BY artists.name ORDER BY popularity DESC LIMIT 5;
```

Find most active user on our platform

```sql 
SELECT first_name, count(*) as song_played FROM songplays JOIN users USING (user_id) GROUP BY first_name ORDER BY song_played DESC LIMIT 5;
```