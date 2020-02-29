"""
This file is for testing one query at a time to see if it works.  Mainly intended for insert commands.
"""

import configparser
import psycopg2

config = configparser.ConfigParser()
config.read('dwh.cfg')

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()

from sql_queries import *

cur.execute(artist_table_insert)
conn.commit()
conn.close()