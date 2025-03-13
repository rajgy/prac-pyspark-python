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

# Create a list of tuples representing employee data
data1 = [(1, "Henry"), (2, "Smith"), (3, "Hall")]
# Define the column names for the DataFrame
columns1 = ["id", "name"]
# Convert the list of tuples into an RDD (Resilient Distributed Dataset)
rdd1 = sc.parallelize(data1, 1)
# Convert the RDD into a DataFrame using the specified column names
df1 = rdd1.toDF(columns1)
# Display the contents of the DataFrame
df1.show()

# Create another list of tuples representing salary data
data2 = [(1, 100), (2, 500), (4, 1000)]
# Define the column names for the second DataFrame
columns2 = ["id", "salary"]
# Convert the list of tuples into an RDD
rdd2 = sc.parallelize(data2, 1)
# Convert the RDD into a DataFrame using the specified column names
df2 = rdd2.toDF(columns2)
# Display the contents of the second DataFrame
df2.show()

#Left join
left_join = df1.join(df2,["id"], "left")
left_join.show()
# Replace null values in the "salary" column with 0 using `withColumn`
finaldf = left_join.withColumn("salary", coalesce("salary", lit(0)))
finaldf.show()

