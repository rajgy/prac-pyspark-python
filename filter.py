"""
PySpark Example: Filtering a List of Numbers

This script demonstrates how to use PySpark to process a list of numbers efficiently.
It covers the following concepts:

1. Setting up the PySpark environment.
2. Creating a list of numbers.
3. Converting the list into a Spark RDD (Resilient Distributed Dataset).
4. Applying different filter operations:
   - Basic filtering (keeping numbers greater than a threshold).
   - Filtering even and odd numbers.
   - Filtering numbers within a range.
   - Filtering using multiple conditions.
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

# Step 3: Create a Python list of numbers
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Print the original list
print("\n======== RAW LIST========")
print(numbers)

# Step 4: Convert the list to an RDD
rdd = sc.parallelize(numbers)

# Print the elements of the RDD
print("\n======== RDD LIST========")
print(rdd.collect())

# Step 5: Apply different filters to the RDD

# 5.1 Filter: Keep numbers greater than 5
filtered_rdd = rdd.filter(lambda x: x > 5)
print("\n======== NUMBERS GREATER THAN 5========")
print(filtered_rdd.collect())

# 5.2 Filter: Keep only even numbers
even_rdd = rdd.filter(lambda x: x % 2 == 0)
print("\n======== EVEN NUMBERS========")
print(even_rdd.collect())

# 5.3 Filter: Keep only odd numbers
odd_rdd = rdd.filter(lambda x: x % 2 != 0)
print("\n======== ODD NUMBERS========")
print(odd_rdd.collect())

# 5.4 Filter: Keep numbers between 3 and 7 (inclusive)
in_range_rdd = rdd.filter(lambda x: 3 <= x <= 7)
print("\n======== NUMBERS BETWEEN 3 AND 7========")
print(in_range_rdd.collect())

# 5.5 Filter: Keep numbers that are either greater than 7 or even
complex_filter_rdd = rdd.filter(lambda x: x > 7 or x % 2 == 0)
print("\n======== NUMBERS GREATER THAN 7 OR EVEN========")
print(complex_filter_rdd.collect())

# 5.6 Filter: Keep numbers that are both even and greater than 4
combined_rdd = rdd.filter(lambda x: x % 2 == 0 and x > 4)
print("\n======== EVEN NUMBERS GREATER THAN 4========")
print(combined_rdd.collect())

# Step 3: Create an RDD with numbers
numbers = [1, 5, 8, 12, 15, 20, 25, 30]
rdd_numbers = sc.parallelize(numbers)

print("\n📌 Original Number List:")
print(rdd_numbers.collect())

# 🔹 Example 1: Filter numbers greater than 10
filtered_gt_10 = rdd_numbers.filter(lambda x: x > 10)
print("\n✅ Numbers Greater Than 10:")
print(filtered_gt_10.collect())

# 🔹 Example 2: Filter even numbers
filtered_even = rdd_numbers.filter(lambda x: x % 2 == 0)
print("\n✅ Even Numbers:")
print(filtered_even.collect())

# 🔹 Example 3: Filter odd numbers
filtered_odd = rdd_numbers.filter(lambda x: x % 2 != 0)
print("\n✅ Odd Numbers:")
print(filtered_odd.collect())

# 🔹 Example 4: Filter numbers between 10 and 20
filtered_range = rdd_numbers.filter(lambda x: 10 <= x <= 20)
print("\n✅ Numbers Between 10 and 20:")
print(filtered_range.collect())

# ========================== 2️⃣ Filtering Strings ==========================

# Step 4: Create an RDD with strings
words = ["apple", "banana", "grape", "avocado", "cherry", "blueberry", "mango"]
rdd_words = sc.parallelize(words)

print("\n📌 Original Word List:")
print(rdd_words.collect())

# 🔹 Example 1: Filter words that start with 'a'
filtered_start_a = rdd_words.filter(lambda word: word.startswith("a"))
print("\n✅ Words Starting with 'a':")
print(filtered_start_a.collect())

# 🔹 Example 2: Filter words containing "berry"
filtered_berry = rdd_words.filter(lambda word: "berry" in word)
print("\n✅ Words Containing 'berry':")
print(filtered_berry.collect())

# 🔹 Example 3: Filter words with length greater than 5
filtered_length = rdd_words.filter(lambda word: len(word) > 5)
print("\n✅ Words with More Than 5 Letters:")
print(filtered_length.collect())

# 🔹 Example 4: Filter words that end with 'o'
filtered_end_o = rdd_words.filter(lambda word: word.endswith("o"))
print("\n✅ Words Ending with 'o':")
print(filtered_end_o.collect())

# 🔹 Example 5: Filter words that do NOT contain 'a'
filtered_no_a = rdd_words.filter(lambda word: "a" not in word)
print("\n✅ Words Without 'a':")
print(filtered_no_a.collect())

# ========================== 🎉 Program End ==========================

print("\n🎯 PySpark Filtering Complete!")
