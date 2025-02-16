from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os

# Set the Python interpreter for PySpark
python_path = sys.executable  # Get the current Python interpreter path
os.environ['PYSPARK_PYTHON'] = python_path  # Tell PySpark to use this Python interpreter

# Set Java home directory
os.environ['JAVA_HOME'] = r'C:\Users\ghimi\.jdks\corretto-1.8.0_442'

# Add Spark packages (Cassandra connector)
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 '
    'pyspark-shell'
)

# Configure Spark
conf = SparkConf() \
    .setAppName("pyspark") \
    .setMaster("local[*]")  # Use all available cores

# Initialize SparkContext and SparkSession
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Your Spark application code goes here

# Read data from Cassandra table 'zeyotab' in keyspace 'zeyobron'
cassdf = (
    spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .option("spark.cassandra.connection.host", "localhost")
    .option("spark.cassandra.connection.port", "9042")
    .option("keyspace", "zeyobron")
    .option("table", "zeyotab")
    .load()
)

# Display the data
cassdf.show()

# Write data to Cassandra table 'zeyotab1' in keyspace 'zeyobron'
(
    cassdf.write
    .format("org.apache.spark.sql.cassandra")
    .option("spark.cassandra.connection.host", "localhost")
    .option("spark.cassandra.connection.port", "9042")
    .option("keyspace", "zeyobron")
    .option("table", "zeyotab1")
    .save()
)