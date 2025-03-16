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

df = spark.read.format("json").option("multiline", "true").load(r'D:\BigData\bigdata\bath42\Spark\pyspark\pyspark\complexjson\address1.json')

# Now, we ask the machine to show us the story in a nice table so we can see it clearly.
df.show()

# We also ask the machine to tell us what kind of information is inside the story.
# It will show us the names of the things (like "id", "trainer", etc.) and what type they are (like numbers or words).
df.printSchema()


# # We want to take out the information from these smaller boxes and put them in the main table.
# # This is called "flattening" the story.
flatdata = df. selectExpr(
    "address",
    "address.billing_address.address",
    "address.billing_address.city",
    "address.billing_address.postal_code",
    "address.billing_address.state",
    "age",
    "date_of_birth",
    "email_address",
    "first_name",
    "height_cm",
    "is_alive",
    "last_name as surname"

)

# Now, we ask the machine to show us the new, simpler table.
flatdata.show(5)
#
# # Finally, we ask the machine to tell us what kind of information is in the new table.
# flatdata.printSch