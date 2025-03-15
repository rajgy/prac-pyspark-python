

# Import necessary libraries
from pyspark import SparkConf, SparkContext  # Spark configuration and context
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


data4 = [(1, "raj"),
         (2,"ravi"),
         (3,"sai"),
         (5,"rani")
         ]
cust = spark.createDataFrame(data4, ["id","name"])
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7,"laptop")
]

prod = spark.createDataFrame(data3, ["id","name"])
prod.show()

finaldf = cust.join(prod, cust["id"] == prod["id"], "left_anti")
finaldf.show()

finaldf1 =prod.union(finaldf)
finaldf1.show()
