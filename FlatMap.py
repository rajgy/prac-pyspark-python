"""
PySpark Example: Filtering and Manipulating Strings

This script demonstrates how to use PySpark for processing a list of strings.
It covers the following concepts:

1. Setting up the PySpark environment.
2. Creating a list of strings.
3. Converting the list into a Spark RDD (Resilient Distributed Dataset).
4. Applying different operations on strings:
   - Basic filtering (keeping strings containing a substring).
   - String concatenation.
   - String replacement.
   - Filtering based on string length.
   - Filtering strings that start or end with a specific character.
5. Printing the results at each step.

Key Concepts:
- **PySpark**: A Python library for distributed data processing.
- **RDD (Resilient Distributed Dataset)**: A distributed collection of elements processed in parallel.
- **Filter**: A transformation that selects elements based on a condition.

Requirements:
- PySpark must be installed.
- Java and Hadoop environments must be configured (if running locally).
"""

# Import necessary libraries
from pyspark import SparkConf, SparkContext  # Spark configuration and context
import os  # For setting environment variables
import sys  # For accessing system-specific parameters

# Step 1: Set up environment variables for PySpark
python_path = sys.executable  # Get the current Python interpreter path
os.environ['PYSPARK_PYTHON'] = python_path  # Tell PySpark to use this Python interpreter
os.environ['HADOOP_HOME'] = "hadoop"  # Set Hadoop home directory
os.environ['JAVA_HOME'] = r'C:\Users\ghimi\.jdks\corretto-1.8.0_442'  # Set Java home directory

# Step 2: Configure Spark
conf = SparkConf().setAppName("pyspark").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Print a message to indicate the program has started
print("STARTED=============")


# Create a list of strings containing the "~" separator
listr = ["A~B", "C~D", "E~F"]

# Print the original list
print("\n===== RAW LIST ======")
print(listr)

# Convert the list into an RDD (Resilient Distributed Dataset)
rddstr = sc.parallelize(listr)

# Print the RDD contents
print("\n===== RDD LIST ======")
print(rddstr.collect())

# Use flatMap() to split each string by the "~" separator
# flatMap() applies the split operation to each element and flattens the results into a single RDD
flatdata = rddstr.flatMap(lambda x: x.split("~"))

