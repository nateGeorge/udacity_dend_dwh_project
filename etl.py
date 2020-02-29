import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads raw JSON files into staging tables in the DB in preparation for ETL.
    cur and conn and the curson and connection from the psycopg2 API to the redshift DB.
    """
    for query in copy_table_queries:
        print('executing query: {}'.format(query))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data into star schema DB from staging tables.
    cur and conn and the curson and connection from the psycopg2 API to the redshift DB.
    """
    for query in insert_table_queries:
        print('executing query: {}'.format(query))
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to redshift DB, loads data into staging tables, and runs ETL to put data in a star schema.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()