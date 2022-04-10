import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Copy JSON song and log data from S3 into staging_songs
    and staging_events in sparkifydb database
    Arguments:
        cur: cursor to execute commands.
        conn: connection to the database.
    """
    print("Start loading data to Redshift...")

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

    print('All files copied completed.')


def insert_tables(cur, conn):
    """ Extract data from staging area. Transform the data into appropriate format.
    Then load into the new dimensional table.
    Arguments:
        cur: cursor to execute commands.
        conn: connection to the database.
    """
    print("Start inserting data from staging tables into analysis tables...")

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

    print('All files inserted completed.')


def main():
    """ Connect to the database. Copy data to the staging tables. Insert them into dimensional tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
