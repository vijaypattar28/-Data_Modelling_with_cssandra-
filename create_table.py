from cassandra.cluster import Cluster
from sql_queries import create_table_queries, drop_table_queries


def create_cluster_Keyspace():
    '''
    we create the keyspace with SimpleStrategy and replication factor = 1
    '''
    
    try: 
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
    except Exception as e:
        print(e)
    

    try:
        session.execute(""" 
        CREATE KEYSPACE IF NOT EXISTS udacity WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
        }
        """)
    except Exception as e:
        print(e)
    
    # TO-DO: Set KEYSPACE to the keyspace specified above
    try:
        session.set_keyspace('udacity')
    except Exception as e:
        print(e)
    
    return cluster, session

def drop_tables(session):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            session.execute(query)
        except psycopg2.Error as e:
            print("Error : Dropping table " + query)
            print (e)
            
def create_tables(session):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        session.execute(query)

        
def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the session and cluster connection. 
    """
    cluster, session = create_cluster_Keyspace()
    
    drop_tables(session)
    create_tables(session)

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()