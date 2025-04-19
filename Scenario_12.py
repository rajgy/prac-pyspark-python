# Import necessary libraries
from pyspark import SparkConf, SparkContext  # Spark configuration and context
from pyspark.sql import SparkSession         # Spark session for DataFrame operations
import os                                    # For setting environment variables
import sys                                   # For accessing system-specific parameters
from pyspark.sql.functions import *          # For using SQL functions like `col`, `filter`, etc.
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window

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
    ("C1", "A1", 5000, "Jan"),
    ("C1", "A2", 20000, "Feb"),
    ("C2", "A2", 3000, "Mar"),
    ("C2", "A3", 5000, "Jan"),
    ("C2", "A4", 6000, "Feb"),
    ("C3", "A5", 7000, "Jan"),
    ("C4", "A6", 9000, "Feb")
]

# Define the schema as column names
columns = ["Customer_Id", "Account_ID", "Balance", "Month"]

# Create DataFrame using toDF()
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

from pyspark.sql.functions import *

cnt = df.groupBy("Customer_Id").agg(count("Account_ID").alias("cnt"))

cnt.show()


jind = cnt.withColumn("Account_type",expr("case when cnt > 1 then 'JOINT' else 'IND' end"))

jind.show()


lefjoin = df.join(jind,["Customer_Id"],"left")

lefjoin.show()

from pyspark.sql.window import Window

salarywindow =  Window.partitionBy("Customer_Id").orderBy(col("Balance").desc())

drank = df.withColumn("dr",dense_rank().over(salarywindow))

drank.show()

finaldf = drank.filter("dr=1").drop("dr")
finaldf.show()