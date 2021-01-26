import configparser
import psycopg2
from queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load json files stored in S3 to staging tables
    """
    print('Inserting data from S3 into staging tables...')
    for query in copy_table_queries:
        print(f'Running {query}...')
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transform data from staging tables to dimensional tables
    """
    print('Inserting data from staging tables to dimensional tables...')
    for query in insert_table_queries:
        print(f'Running {query}...')
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
