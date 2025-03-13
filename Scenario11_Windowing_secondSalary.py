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


# Create a List of Tuples
data = [
    ("DEPT1", 1000),
    ("DEPT1", 700),
    ("DEPT1", 500),
    ("DEPT2", 400),
    ("DEPT2", 200),
    ("DEPT3", 500),
    ("DEPT3", 200)]

# Create columns as per List to tuples requires
columns = ["dept", "salary"]

# Creating List of Tuples into Dataframe for further data processing
df = spark.createDataFrame(data,columns)
df.show()

#==STEP 1== CREATE THE WINDOW
deptwindow = Window.partitionBy("dept").orderBy(col("salary").desc())

#==STEP 2===APPLYING WITH WINDOW ON DATAFRAME TO DENSE RANK
drank = df.withColumn("drank",dense_rank().over(deptwindow))
drank.show()

#===STEP 3=== FILTER RANK =2
filrank = drank.filter("drank=2")
filrank.show()

finaldf = filrank.drop("drank")
finaldf.show()