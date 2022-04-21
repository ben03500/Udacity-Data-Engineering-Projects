# Project 4: Create a Data Lake with Spark

This is my forth project as part of the Udacity Data Engineering Nanodegree course. This Sparkify data lake contains data from song and users log data in order to help analytical process in our business needs. This data lake ensure that the data is properly organized, integral and can be easily access with queries. We loaded the source data from S3, process the data into analytics tables using Spark, and load them back into S3 which it is considered our data lake.

## Partition parquet files on S3

This analytic data lake contains 5 following tables:

### Fact table

1. ```songplays```

|  songplays  |    type   |
|-------------|-----------|
| songplay_id | INT       |
| start_time  | TIMESTAMP |
| user_id     | INT       |
| level       | VARCHAR   |
| song_id     | VARCHAR   |
| artist_id   | VARCHAR   |
| session_id  | INT       |
| location    | TEXT      |
| user_agent  | TEXT      |

### Dimension tables

2. ```users```

|    users   |   type  |
|------------|---------|
| user_id    | INT     |
| first_name | VARCHAR |
| last_name  | VARCHAR |
| gender     | CHAR(1) |
| level      | VARCHAR |

3. ```songs```

|   songs   |   type  |
|-----------|---------|
| song_id   | VARCHAR |
| title     | VARCHAR |
| artist_id | VARCHAR |
| year      | INT     |
| duration  | FLOAT   |

4. ```artists```

|   artists  |   type  |
|------------|---------|
| artist_id  | VARCHAR |
| name       | VARCHAR |
| location   | TEXT    |
| latitude   | FLOAT   |
| logitude   | FLOAT   |

5. ```time```

|    time    |    type   |
|------------|-----------|
| start_time | TIMESTAMP |
| hour       | INT       |
| day        | INT       |
| week       | INT       |
| month      | INT       |
| year       | INT       |
| weekday    | VARCHAR   |

This design is similar to what call "Star-schema", where the `songplays` is the fact table that contain 'fact' about when customers played a song. And the rest of the tables are dimension tables that contains additional information that referred in fact table. i.e., `users` table contains supplement details about each user.

This schema might be denormalized the data a bit, but it help simplify query and improve query performance.
