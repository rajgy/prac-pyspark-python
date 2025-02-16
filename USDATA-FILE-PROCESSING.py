from pyspark import SparkConf, SparkContext  # Import necessary PySpark classes
import os  # Import os module for environment variables
import sys  # Import sys module for system-related operations

# Set up environment variables for PySpark
python_path = sys.executable  # Get the current Python executable path
os.environ['PYSPARK_PYTHON'] = python_path  # Set the PYSPARK_PYTHON environment variable to the current Python interpreter
os.environ['HADOOP_HOME'] = "hadoop"  # Set the HADOOP_HOME environment variable to the Hadoop directory
os.environ['JAVA_HOME'] = r'C:\Users\ghimi\.jdks\corretto-1.8.0_442'  # Set the JAVA_HOME environment variable to the Java installation path

# Configure Spark
conf = SparkConf().setAppName("pyspark").setMaster("local[*]")  # Create a SparkConf object and configure the app name and master URL
sc = SparkContext(conf=conf)  # Create a SparkContext with the given configuration to interface with the Spark cluster

print("STARTED=============")  # Print a message indicating that the process has started

# Read the file and create an RDD
data = sc.textFile("usdata.csv")  # Create an RDD from a CSV file ("usdata.csv")

# Print the raw data from the RDD
print("===== RAW DATA====")
data.foreach(print)  # Print each element of the raw RDD individually (can be costly for large datasets)

# Filter data where the length of each element is greater than 200 characters
lendata = data.filter(lambda x: len(x) > 200)  # Keep elements whose length is greater than 200
print("===== lendata DATA====")
lendata.foreach(print)  # Print each element of the filtered RDD

# Split the filtered data into individual components by comma (flatMap)
flatdata = lendata.flatMap(lambda x: x.split(","))  # Split each element by commas and flatten the result
print("===== flatdata DATA====")
flatdata.foreach(print)  # Print each element of the flattened RDD

# Remove dashes from the data
remdata = flatdata.map(lambda x: x.replace("-", ""))  # Remove all dashes from each element
print("===== remdata DATA====")
remdata.foreach(print)  # Print each element of the transformed RDD

# Append ",zeyo" to each element of the transformed RDD
condata = remdata.map(lambda x: x + ",zeyo")  # Concatenate ",zeyo" to each element
print("===== condata DATA====")
condata.foreach(print)  # Print each element of the final transformed RDD
