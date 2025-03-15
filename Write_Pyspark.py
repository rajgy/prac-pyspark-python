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

# Reading the usdata.csv file from the local storge of laptop

df = spark.read.format("csv").option("header","true").load(r'D:\BigData\bigdata\bath42\Spark\pyspark\pyspark\usdata.csv')
df.show()


#Write the usdata.csv file into usdata.json format in same local storage
df.write.format("json").mode("append").save(r'D:\BigData\writeData\jsonusdata')
# df.write.format("json").mode("append").save(r'D:\BigData\writeData\jsonusdata1')
# df.write.format("json").mode("error").save(r'D:\BigData\writeData\jsonusdata2')
 #

# Reading parquet file format
parquet =  spark.read.format("parquet").load("file:///D:/BigData/bigdata/bath42/Spark/pyspark/pyspark/file5.parquet")
parquet.show()

#Write the parquet format to CSV
parquet.write.format("CSV").mode("overwrite").save("file:///D:/BigData/writeData/CSVFILE")

# # Reading XML file format
# xml = spark.read.format("com.databricks.spark.xml").option("rowTag", "row").load("file:///D:/BigData/bigdata/bath42/Spark/pyspark/pyspark/large-dataset.xml")
# xml.show()
#
# # Write the XML format to CSV
# xml.write.format("csv").mode("overwrite").save("file:///D:/BigData/writeData/XMLFILE")


print("\n======Converted and SAVED THE CSV FORMAT DATA INTO JSON FORMAT====\n")