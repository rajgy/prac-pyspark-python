# Import necessary libraries

from pyspark import SparkConf, SparkContext  # Spark configuration and context
from pyspark.sql import SparkSession  # Spark session for DataFrame operations
import os  # For setting environment variables
import sys  # For accessing system-specific parameters
from pyspark.sql.functions import *


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


data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

cust = spark.createDataFrame(data4, ["id", "name"]).coalesce(1)
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]



prod = spark.createDataFrame(data3, ["id", "product"]).coalesce(1)
prod.show()








innerjoin = cust.join(prod , ["id"] , "inner")

print("======INNER JOIN======")
print()
innerjoin.show()



left = cust.join(prod, ["id"], "left")

print("======left JOIN======")
print()
left.show()


right = cust.join(prod, ["id"] , "right")

print("======right JOIN======")
print()
right.show()



full = cust.join(prod, ["id"] , "full" )

print("======full JOIN======")
print()
full.show()


prod1 = spark.createDataFrame(data3, ["id1", "product"]).coalesce(1)
prod1.show()


inner = cust.join(prod1 ,   cust["id"] == prod1["id1"]    , "inner" ).drop("id1")
inner.show()