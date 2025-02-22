# Import necessary libraries
from pyspark import SparkConf, SparkContext  # Spark configuration and context
from pyspark.sql import SparkSession  # Spark session for DataFrame operations
import os  # For setting environment variables
import sys  # For accessing system-specific parameters

# Step 1: Set up environment variables for PySpark
python_path = sys.executable  # Get the current Python interpreter path
os.environ['PYSPARK_PYTHON'] = python_path  # Tell PySpark to use this Python interpreter
os.environ['HADOOP_HOME'] = "hadoop"  # Set Hadoop home directory
os.environ['JAVA_HOME'] = r'C:\Users\ghimi\.jdks\corretto-1.8.0_442'  # Set Java home directory

# Step 2: Configure Spark
conf = SparkConf().setAppName("pyspark").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("pyspark") \
    .getOrCreate()

# Print a message to indicate the program has started
print("STARTED=============")

# DATAFRAME SHOW

data = sc.textFile("dt.txt")
print()
print("======RAW DATA======")
data.foreach(print)

# step 2 ===================

mapsplit = data.map(lambda x : x.split(","))

# Step 3 ===================

from collections import namedtuple

columns = namedtuple('columns',['id','tdate','amt','category','product','mode'])

# Step 4 =====================

schemardd = mapsplit.map(lambda x : columns(x[0],x[1],x[2],x[3],x[4],x[5]))

# Step 5 =====================

colfilter = schemardd.filter(lambda x : 'Gymnastics' in x.product)

print()
print("======colfilter DATA======")
colfilter.foreach(print)

# Convert RDD to DataFrame
df = colfilter.toDF()
df.show()