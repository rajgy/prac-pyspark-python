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
data = [
    ("1", "a", "10000"),
    ("2", "b", "5000"),
    ("3", "c", "15000"),
    ("4", "d", "25000"),
    ("5", "e", "50000"),
    ("6", "f", "7000")
]

myschema = ["empid","name","salary"]
df = spark.createDataFrame(data,schema=myschema)
df.show()

#add a constant value to a new column for all rows
finaldf = df.withColumn("Designation", lit('Employee'))
finaldf.show()

finaldf_condition = df.withColumn("Designation",
                        when(df["salary"] > 15000, "Senior Staff")
                        .otherwise("Junior Staff"))
finaldf_condition.show()