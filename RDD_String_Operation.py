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

# Step 3: Create a Python list of strings
string_list = ["zeyobron", "zeyo", "byte", "analytics", "data", "science"]

# Print the original list
print("\n======== RAW STRING LIST ========")
print(string_list)

# Step 4: Convert the list to an RDD
rdd_str = sc.parallelize(string_list)

# Print the elements of the RDD
print("\n======== RDD STRING LIST ========")
print(rdd_str.collect())

# Step 5: Apply different operations on strings

# 5.1 Concatenate a suffix to each string
concat_rdd = rdd_str.map(lambda x: x + "_Analytics")
print("\n======== CONCATENATED STRINGS ========")
print(concat_rdd.collect())

# 5.2 Filter: Keep only strings that contain 'zeyo'
zeyo_rdd = rdd_str.filter(lambda x: "zeyo" in x.lower())
print("\n======== STRINGS CONTAINING 'ZEYO' ========")
print(zeyo_rdd.collect())

# 5.3 Replace 'zeyo' with 'tera' in strings
replace_rdd = rdd_str.map(lambda x: x.replace("zeyo", "tera"))
print("\n======== STRINGS AFTER REPLACEMENT ========")
print(replace_rdd.collect())

# 5.4 Filter: Keep only strings with more than 4 characters
length_rdd = rdd_str.filter(lambda x: len(x) > 4)
print("\n======== STRINGS WITH MORE THAN 4 CHARACTERS ========")
print(length_rdd.collect())

# 5.5 Filter: Keep only strings that start with 'd'
start_d_rdd = rdd_str.filter(lambda x: x.startswith("d"))
print("\n======== STRINGS STARTING WITH 'D' ========")
print(start_d_rdd.collect())

# 5.6 Filter: Keep only strings that end with 'a'
end_a_rdd = rdd_str.filter(lambda x: x.endswith("a"))
print("\n======== STRINGS ENDING WITH 'A' ========")
print(end_a_rdd.collect())

# End of script


