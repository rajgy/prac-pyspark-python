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



csvdf = spark.read.format("csv").option("header","true").load("usdata.csv")

print()

print("======== CSV DF==============")

print()

csvdf.show()





parquetdf =  spark.read.format("parquet").load("file5.parquet")

print()

print("======== parquetdf ==============")

print()

parquetdf.show()





orcdf =   spark.read.format("orc").load("data.orc")

print()

print("======== orcdf ==============")

print()

orcdf.show()



jsondf =  spark.read.format("json").load("file4.json")

print()

print("======== jsondf ==============")

print()

jsondf.show()


