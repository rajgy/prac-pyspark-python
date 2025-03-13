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



# Create a list of tuples representing customer data
data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

# Create a Spark DataFrame from the list of tuples
cust = spark.createDataFrame(data4, ["id", "name"]).coalesce(1)

# Display the contents of the DataFrame
cust.show()

# Create another list of tuples representing product data
data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

# Create a Spark DataFrame from the list of tuples
prod = spark.createDataFrame(data3, ["id", "product"]).coalesce(1)
prod.show()


# Extract the "id" column from the product DataFrame and convert it to a list
prodlist = prod.select("id").rdd.flatMap(lambda x: x).collect()
print(prodlist)

print()
print()

# Filter the customer DataFrame to exclude rows where the "id" is in the product list
fildf = cust.filter(~col("id").isin(prodlist))
fildf.show()


# Perform an anti-join between the customer and product DataFrames
antijoin = cust.join(prod, ["id"], "left_anti")
antijoin.show()