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

# Full RDD operations
print("=====STARTED======")

# Read the file and create an RDD
data = sc.textFile("state.txt")  # Create an RDD from a text file ("state.txt")

# Print the collected data from the RDD
print("====== FILE RDD======")
print(data.collect())  # Collect and print all elements of the RDD
data.foreach(print)  # Print each element of the RDD individually

# Apply flatMap to split the elements of the RDD by the delimiter "~"
flatten = data.flatMap(lambda x: x.split("~"))  # Use flatMap to split each line by "~" into multiple elements
print("===== flatten LIST======")
print(flatten.collect())  # Collect and print the flattened RDD
flatten.foreach(print)  # Print each element of the flattened RDD individually

# Filter the RDD to only include elements containing the word "State"
filstate = flatten.filter(lambda x: 'State' in x)  # Filter the RDD to include only elements with the word "State"
print("===== filstate LIST======")
print(filstate.collect())  # Collect and print the filtered RDD
filstate.foreach(print)  # Print each element of the filtered RDD individually

# Map the RDD to remove the prefix "State->" from each element
states = filstate.map(lambda x: x.replace("State->", ""))  # Map the RDD to remove the "State->" prefix
print("===== states LIST======")
print(states.collect())  # Collect and print the mapped RDD
states.foreach(print)  # Print each element of the mapped RDD individually

# Filter the RDD to only include elements containing the word "City"
filcity = flatten.filter(lambda x: 'City' in x)  # Filter the flattened RDD to include only elements with the word "City"
print("===== filcity LIST======")
print(filcity.collect())  # Collect and print the filtered RDD for cities
filcity.foreach(print)  # Print each element of the filtered RDD for cities individually

# Map the RDD to remove the prefix "City->" from each element
cities = filcity.map(lambda x: x.replace("City->", ""))  # Map the RDD to remove the "City->" prefix
print("===== cities LIST======")
print(cities.collect())  # Collect and print the mapped RDD for cities
cities.foreach(print)  # Print each element of the mapped RDD for cities individually


#  full rdd operations

print("=====STARTED======")


data = sc.textFile("state.txt")
print()
print("====== FILE RDD======")
print(data.collect())
data.foreach(print)



flatten = data.flatMap(lambda x : x.split("~"))
print()
print("===== flatten LIST======")
print(flatten.collect())
flatten.foreach(print)



filstate = flatten.filter(lambda x : 'State' in x)
print()
print("===== filstate LIST======")
print(filstate.collect())
filstate.foreach(print)



states = filstate.map(lambda x : x.replace("State->",""))
print()
print("===== states LIST======")
print(states.collect())
states.foreach(print)


filcity =  flatten.filter(lambda x : 'City' in x)
print()
print("===== filcity LIST======")
print(filcity.collect())
filcity.foreach(print)


cities = filcity.map(lambda x : x.replace("City->",""))
print()
print("===== cities LIST======")
print(cities.collect())
cities.foreach(print)