#!/usr/bin/env bash
dcos spark run --docker-image=markfjohnson/spark_pandas --submit-args="https://raw.githubusercontent.com/markfjohnson/Cassandra_PySpark_Examples/master/read_and_write_s3.py" --verbose