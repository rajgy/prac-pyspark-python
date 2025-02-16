from collections import namedtuple

from pyspark import SparkConf, SparkContext  # Import necessary PySpark classes
import os  # Import os module for environment variables
import sys  # Import sys module for system-related operations
from pyspark.sql.functions import col, sum, avg, count
# Set up environment variables for PySpark
python_path = sys.executable  # Get the current Python executable path
os.environ['PYSPARK_PYTHON'] = python_path  # Set the PYSPARK_PYTHON environment variable to the current Python interpreter
os.environ['HADOOP_HOME'] = "hadoop"  # Set the HADOOP_HOME environment variable to the Hadoop directory
os.environ['JAVA_HOME'] = r'C:\Users\ghimi\.jdks\corretto-1.8.0_442'  # Set the JAVA_HOME environment variable to the Java installation path

# Configure Spark
conf = SparkConf().setAppName("pyspark").setMaster("local[*]")  # Create a SparkConf object and configure the app name and master URL
sc = SparkContext(conf=conf)  # Create a SparkContext with the given configuration to interface with the Spark cluster

print("STARTED=============")  # Print a message indicating that the process has started

# Step 1: Read the file
data = sc.textFile("dt.txt")
print("\n=======raw data=====")
data.foreach(print)

#Step 2: Split with Comma (MapSplit)
mapsplit = data.map(lambda x : x.split(","))
print("\n=======MapSplit======")
mapsplit.foreach(print)


# Step 3: Define Columns
columns = namedtuple('columns',['tno','tdata','amount','category','product','mode'])
print("\n=====Colums Define====")



# Step 4: Impose easch data split to the columns
schemardd = mapsplit.map(lambda x: columns(x[0],x[1],x[2],x[3],x[4],x[5]))
print("\n=====Schema Rdd=====\n")
schemardd.foreach(print)

# Step 5: Filter Required product Column
prodfilter = schemardd.filter(lambda x : 'Gymnastics' in x.product)
print("\n===Product Filter=====\n")
prodfilter.foreach(print)