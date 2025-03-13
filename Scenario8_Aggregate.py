# Import necessary libraries
from pyspark import SparkConf, SparkContext  # Spark configuration and context
from pyspark.sql import SparkSession         # Spark session for DataFrame operations
import os                                    # For setting environment variables
import sys                                   # For accessing system-specific parameters
from pyspark.sql.functions import *          # For using SQL functions like `col`, `filter`, etc.

# Step 1: Set up environment variables for PySpark
python_path = sys.executable                          # Get the current Python interpreter path
os.environ['PYSPARK_PYTHON'] = python_path            # Tell PySpark to use this Python interpreter
os.environ['HADOOP_HOME'] = "hadoop"                   # Set Hadoop home directory
os.environ['JAVA_HOME'] = r'C:\Users\cogni\.jdks\corretto-1.8.0_442'  # Set Java home directory


# Step 2: Configure Spark
conf = SparkConf().setAppName("pyspark").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("pyspark") \
    .getOrCreate()

# Print a message to indicate the program has started
print("STARTED=============")


# Define the data as a list of tuples
data = [
    ('sai', 'chn', 1),
    ('sai', 'hyd', 2),
    ('sai', 'chn', 2),
    ('sai', 'hyd', 1),
    ('zeyo', 'chn', 2),
    ('zeyo', 'hyd', 3),
    ('zeyo', 'chn', 2),
    ('zeyo', 'hyd', 1)
]


# Create a DataFrame using the data and specifying the column names
df = spark.createDataFrame(data, ["name", "city", "amount"]).coalesce(1)

# Show the DataFrame
df.show()

print("======== SUM PER EACH NAME=========")

# Performing agg function to sum the amount according to name while amount receive by same name from different cites
aggdf1 = df.groupBy("name").agg(sum("amount").alias("total_amount"))
aggdf1.show()