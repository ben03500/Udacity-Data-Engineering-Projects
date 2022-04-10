import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Drop any existing tables from the database.
    Arguments:
        cur: cursor the allows to execute SQL commands.
        conn: connection to the database.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    print("Dropped all tables successfully.")


def create_tables(cur, conn):
    """ Create new tables: songplays, users, artists, songs, time.
    Arguments:
        cur: cursor the allows to execute SQL commands.
        conn: connection to the database.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

    print("Created tables successfully.")


def main():
    """ Connect to the database. Drop any existing tables. Create new tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
