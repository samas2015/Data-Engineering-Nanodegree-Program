import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    loads s3 data into staging tables, calls copy queries from sql_queries.py
    '''
    counter=0;
    for query in copy_table_queries:
        counter=counter+1;
        print("Copying tables from s3 to staging tables ",counter)
        cur.execute(query)
        conn.commit()



def insert_tables(cur, conn):
    '''
    Inserts data into Redshift tables, calls insert queries from sql_queries.py
    '''
    counter=0;
    for query in insert_table_queries:
        counter=counter+1;
        print("Inserting data into Redshift tables ",counter)
        cur.execute(query)
        conn.commit()
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #connect to our cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()