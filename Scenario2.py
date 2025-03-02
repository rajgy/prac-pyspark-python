

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




from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder.appName("Hierarchy").getOrCreate()

# Sample Data
data = [("A", "AA"),
        ("B", "BB"),
        ("C", "CC"),
        ("AA", "AAA"),
        ("BB", "BBB"),
        ("CC", "CCC")]

# Create DataFrame
df = spark.createDataFrame(data, ["child", "parent"])
df.show()

# Rename columns in the second DataFrame to avoid ambiguity
df2 = df.withColumnRenamed("child", "parent_child").withColumnRenamed("parent", "GrandParent")
df2.show()

##Performing inner join between df and df2
inner =df.join(df2, df["parent"]==df2["parent_child"], "inner")
inner.show()

## using to drop the one extra, which is required for our scenario solving
finaldf = inner.drop("parent_child")
finaldf.show()


