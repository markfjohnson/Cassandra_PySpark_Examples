from cassandra.cluster import Cluster
from cassandra.query import *
from pyspark import SparkContext
from cassandra import ConsistencyLevel

KEYSPACE="Financial"

sc = SparkContext(appName="PySpark Cassandra File Write Example")

cluster = Cluster(['node-0.cassandra.mesos:9042', 'node-1.cassandra.mesos:9042', 'node-0.cassandra.mesos:9042'])  # provide contact points and port

# Verify Tables and Keyspaces exist for data if not then create it
session = cluster.connect()

session.execute("""
     CREATE KEYSPACE %s
     WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
     """ % KEYSPACE)

session.execute("""
      CREATE TABLE stock_prices (
          thekey text,
          col1 text,
          col2 text,
          PRIMARY KEY (thekey, col1)
      )
      """)

# Run the batch to read a block of input file rows and then prepare a cassandra write batch

insert_user = session.prepare("INSERT INTO users (name, age) VALUES (?, ?)")
batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

for (name, age) in users_to_insert:
    batch.add(insert_user, (name, age))

session.execute(batch)

rows = session.execute('select * from table limit 5;')
for row in rows:
    print row.id