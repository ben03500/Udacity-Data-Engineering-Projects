# Project: Data Modeling with Postgres

This is my first project as part of the Udacity Data Engineering Nanodegree course.

## The purposes of this database

This Sparkify internal SQL database was created from song and users log dataset to help analytical process in our business needs. This SQL database ensure that the data is properly organized, integral and can be easily access with queries.

## Database schema design

This database contains 5 following tables:

1. ```songplays```
2. ```users```
3. ```songs```
4. ```artists```
5. ```time```

This design is similar to what call "Star-schema", where the `songplays` is the fact table that contain 'fact' about when customers played a song. And the rest of the tables is dimension tables that contains additional information that referred in fact table. i.e., `users` table contains supplement details about each user.

This schema denormalized the data a bit, but it help simplify query and improve query performance.

## ETL pipeline

This ETL pipeline consists of 3 steps: (1) Extraction: the song and users log data is extracted from JSON files. (2) Transformation: There are not a lot of transformations involved in this scenario, except for `time` table where the timestamp need to be converted into appropriate format & in `songplays` table where the `artist_id` and `song_id` need to be retrive from another table. And (3) Load: The transformed data is loaded onto the SQL database.

Perform the ETL process:

```bash
$ python3 create_tables.py
$ python3 etl.py
```

To update this database; the log data might be collect and perform ETL on a daily basis. And the song data might also be collect every month or when artist release a new song.

## Sample SQL queries

Find most popular artist on Sparkify

```sql
SELECT artists.name, count(*) as popularity FROM songplays JOIN artists USING (artist_id) GROUP BY artists.name ORDER BY popularity;
```
Find most active user on our platform

```sql 
SELECT user_id, count(*) as song_played FROM songplays GROUP BY user_id ORDER BY song_played desc LIMIT 5;
```