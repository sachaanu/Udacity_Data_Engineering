import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, copy_table_order, insert_table_order, check_queries, check_query_order
import time
import datetime


def load_staging_tables(cur, conn):
    """
    To load data from logs (S3 bucket) to staging tables in redshift
    :param cur: the cursor of the connection
    :param conn: the connection to DB itself
    :return: None
    
    """
    print('Loading data in Staging Tables')
    starting = datetime.datetime.now()
    cnt = 0
    for query in copy_table_queries:
        print('Copying data for table {} -- '.format(copy_table_order[cnt]))
        print('Running' + query)
        cur.execute(query)
        conn.commit()
        cnt += 1
    finished = datetime.datetime.now()
    print(' ------- Data load to staging tables strated {} and completed at {} --------'.format(starting,finished))


def insert_tables(cur, conn):
    """
    To insert data from staging tables to ETL-DW tables
    :param cur: the cursor to the connection
    :param conn: the connection to DB
    return: None
    """
    starting = datetime.datetime.now()
    print('Inserting data from staging tables into analytics tables')
    cnt = 0
    for query in insert_table_queries:
        print('Inserting data for table {} -- '.format(insert_table_order[cnt]))
        print('Running' + query)
        cur.execute(query)
        conn.commit()
        cnt += 1
    finished = datetime.datetime.now()
    print(' ------- Data insert for all tables strated {} and completed at {} --------'.format(starting,finished))


def run_analytic_check(cur, conn):
    """
    To verify the count of rows inserted in the DWH tables
    :param cur: the cursor to the connection
    :param conn: the connection to DB
    return: count of rows for the query
    """
    for query in check_queries:
        print('Running count of -- {} -- '.format(query))
        cur.execute(query)
        res = cur.fetchone()
        for row in res:
            print(" ",row)
    print(' ----- Analytic check for all tables ----- ')

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    run_analytic_check(cur,conn)

    conn.close()


if __name__ == "__main__":
    main()