# Import necessary libraries
from pyspark import SparkConf, SparkContext  # Spark configuration and context
from pyspark.sql import SparkSession         # Spark session for DataFrame operations
import os                                    # For setting environment variables
import sys                                   # For accessing system-specific parameters
from pyspark.sql.functions import *          # For using SQL functions like `col`, `filter`, etc.
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

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

# Define the schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("subject", StringType(), True)
])

# Create data
data = [
    (1, "Spark"),
    (1, "Scala"),
    (1, "Hive"),
    (2, "Scala"),
    (3, "Spark"),
    (3, "Scala")
]

# Create DataFrame
df = spark.createDataFrame(data, schema).coalesce(1)

# Show the DataFrame
df.show()


colagg = df.groupBy("id").agg(collect_list("subject").alias("subject"))
colagg.show()


colagg.printSchema()


strcon = colagg.withColumn("subject",expr("cast(subject as string)"))
strcon.show()
strcon.printSchema()


repdata = strcon.withColumn("subject",expr("replace(subject,'[','')")).withColumn("subject",expr("replace(subject,']','')"))
repdata.show()
repdata.printSchema()