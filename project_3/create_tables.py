import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all tables listed in drop_table_queries
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all tables for ETL including staging and final star-schema based table.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    main to connect to Amazon Redshift DB and to start by droping tables and creating new tables.  Code assumes cluster has been created.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()