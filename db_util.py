import sys
import logging
import psycopg2

# rds settings
endpoint  = 'mfi-dev-env.cluster-cvziyfxrgf0b.us-west-2.rds.amazonaws.com'
dbuser = 'postgres'
password = '55EUDMgSDUnmQ0x0YLhZ'
database = 'mi_datasource'
port = 5432

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def make_connection():
    try:
        conn_str="host={0} dbname={1} user={2} password={3} port={4}".format(
            endpoint,database,dbuser,password,port)
        conn = psycopg2.connect(conn_str)
    except:
        logger.error("ERROR: Could not connect to Postgres instance.")
        sys.exit()
    return conn

cursor = make_connection().cursor()

def get_thing_id(val):
    cursor.execute('select thing_id from thing_property where property_name = %s and property_value = %s', val)
    result = cursor.fetchone()
    return result
    
def get_thing_property_value(val):
    cursor.execute("select property_value from thing_property where thing_id = %s and property_name = %s", val)
    return cursor.fetchall()
    

    
