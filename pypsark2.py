from pyspark import SparkConf, SparkContext
import os
import sys

# Set up environment variables for PySpark
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = "hadoop"
os.environ['JAVA_HOME'] = r'C:\Users\ghimi\.jdks\corretto-1.8.0_442'

# Configure Spark
conf = SparkConf().setAppName("pyspark").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("STARTED=============")

# Creating a simple list
lis = [1, 2, 3, 4]
print("Initial List:", lis)

# Converting list to RDD
rddin = sc.parallelize(lis)
print("===== RAW RDD LIST=====")
print(rddin.collect())

# Transformations on RDD
addrdd = rddin.map(lambda x: x + 2)
print("===== addrdd RDD LIST=====")
print(addrdd.collect())

mulrdd = rddin.map(lambda x: x * 10)
print("===== mulrdd RDD LIST=====")
print(mulrdd.collect())

# Stop the SparkContext
sc.stop()

# Instructions for IntelliJ Setup:
# 1. Install IntelliJ IDEA and set up the Python plugin.
# 2. Install Apache Spark and set up environment variables (HADOOP_HOME, JAVA_HOME).
# 3. Install PySpark using 'pip install pyspark'.
# 4. Run this script in IntelliJ as a Python project.
