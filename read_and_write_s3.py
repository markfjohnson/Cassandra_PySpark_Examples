from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import SQLContext


import_file = "s3a://mark-johnson/CCL.csv"

sc = SparkContext(appName="PySpark S3 File ReadWrite Example")
lines = sc.textFile(import_file)
print(lines.count())