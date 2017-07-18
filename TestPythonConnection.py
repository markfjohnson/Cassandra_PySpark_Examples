from pyspark import SQLContext

from pyspark import *
from cassandra.cluster import Cluster

sc = SQLContext()

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf


spark.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))

// set params for the particular cluster
spark.setCassandraConf("Cluster1", CassandraConnectorConf.ConnectionHostParam.option("127.0.0.1") ++ CassandraConnectorConf.ConnectionPortParam.option(12345))
spark.setCassandraConf("Cluster2", CassandraConnectorConf.ConnectionHostParam.option("127.0.0.2"))

// set params for the particular keyspace
spark.setCassandraConf("Cluster1", "ks1", ReadConf.SplitSizeInMBParam.option(128))
spark.setCassandraConf("Cluster1", "ks2", ReadConf.SplitSizeInMBParam.option(64))
spark.setCassandraConf("Cluster2", "ks3", ReadConf.SplitSizeInMBParam.option(80))

cluster = Cluster(["node-0.cassandra.mesos:9042"])


conf = SparkConf()
conf.set("spark.cassandra.connection.host", )
#conf.set("spark.cassandra.auth.username", "cassandra")
#conf.set("spark.cassandra.auth.password", "cassandra")

sc = SparkContext(appName="WriteCassandra")
sqlContext = SQLContext(sc)

connector = CassandraConnector(sc.getConf)

./dcos spark run --submit-args="--properties-file coarse-grained.conf --class portal.spark.cassandra.app.ProductModelPerNrOfAlerts http://marathon-lb-default.marathon.mesos:10018/jars/spark-cassandra-assembly-1.0.jar"


