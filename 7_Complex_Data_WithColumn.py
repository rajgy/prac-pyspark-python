# Import necessary libraries
from pyspark import SparkConf, SparkContext  # Spark configuration and context
from pyspark.sql import SparkSession         # Spark session for DataFrame operations
import os                                    # For setting environment variables
import sys                                   # For accessing system-specific parameters
from pyspark.sql.functions import *          # For using SQL functions like `col`, `filter`, etc.
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from setuptools.command.egg_info import overwrite_arg

# Step 1: Set up environment variables for PySpark
python_path = sys.executable                          # Get the current Python interpreter path
os.environ['PYSPARK_PYTHON'] = python_path            # Tell PySpark to use this Python interpreter
os.environ['HADOOP_HOME'] = "hadoop"                 # Set Hadoop home directory
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

# Imagine we have a magical box called "jsondata" that contains a story written in a special language called JSON.
# JSON is like a way to write down information in a neat and organized way.

df = spark.read.format("json").option("multiline", "true").load(r'D:\BigData\bigdata\bath42\Spark\pyspark\pyspark\complexjson\array_users.json')

# Now, we ask the machine to show us the story in a nice table so we can see it clearly.
df.show()

#print the schema while processing complex data
df.printSchema()

#flatenning the JSON format complex data using with withColumn

# Flatten the nested structure
flattened_df =(df.withColumn("user", expr("results[0].user"))
    .withColumn("DNI", expr("user.DNI"))
    .withColumn("cell", expr("user.cell"))
    .withColumn("dob", expr("user.dob"))
    .withColumn("email", expr("user.email"))
    .withColumn("gender", expr("user.gender"))
    .withColumn("city", expr("user.location.city"))
    .withColumn("state", expr("user.location.state"))
    .withColumn("street", expr("user.location.street"))
    .withColumn("zip", expr("user.location.zip"))
    .withColumn("md5", expr("user.md5"))
    .withColumn("first_name", expr("user.name.first"))
    .withColumn("last_name", expr("user.name.last"))
    .withColumn("title", expr("user.name.title"))
    .withColumn("password", expr("user.password"))
    .withColumn("phone", expr("user.phone"))
    .withColumn("picture_large", expr("user.picture.large"))
    .withColumn("picture_medium", expr("user.picture.medium"))
    .withColumn("picture_thumbnail", expr("user.picture.thumbnail"))
    .withColumn("registered", expr("user.registered"))
    .withColumn("salt", expr("user.salt"))
    .withColumn("sha1", expr("user.sha1"))
    .withColumn("sha256", expr("user.sha256"))
    .withColumn("username", expr("user.username"))
    .drop("results", "user")
)

# Show the flattened DataFrame
flattened_df.show()

flattened_df.printSchema()

