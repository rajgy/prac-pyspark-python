import os
import sys

# Set paths properly
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk1.8.0_202'
os.environ['PATH'] += os.pathsep + r"C:\hadoop\bin"

# Ensure all PySpark dependencies are loaded correctly
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,'
    'org.apache.spark:spark-avro_2.12:3.5.4,'
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'
)

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host", "localhost")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()
print("\nSTARTED=============\n")

# Kafka Stream Processing
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "deepseek4")
    .option("startingOffsets", "earliest")
    .load()
    .withColumn("value", expr("cast(value as string)"))
    .withColumn("value", expr("concat(value, '~RAJJJ')"))
)

# Write Stream to Console with Checkpoint Directory
query = kafka_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "file:///tmp/kafka-checkpoints") \
    .start()

query.awaitTermination()

