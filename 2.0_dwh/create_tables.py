import configparser
import psycopg2
from queries import drop_table_queries, create_table_queries


def drop_tables(cur, conn):
    """
    Delete any pre-existing tables
    """
    print('Dropping tables...')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create staging and dimensional tables
    """
    for query in create_table_queries:
        print(f'Running {query}...')
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to cluster and set up the database tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected to cluster...')

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
