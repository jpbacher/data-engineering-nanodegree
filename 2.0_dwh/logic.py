import configparser
import psycopg2
from queries import table_count_queries


def get_table_counts(cur, conn):
    """
    Retrieve number of instances in each newly created table
    """
    for query in table_count_queries:
        print(f'Running {query}...')
        cur.execute(query)
        final_result = cur.fetchone()
        for row in final_result:
            print(' ', row)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    get_table_counts(cur, conn)

    conn.close()


if __name__ == '__main__':
    main()
