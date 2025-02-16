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

# Basic Python operations
a = 2
print(a)

b = a + 2
print(b)

c = "zeyobron"
print(c)

d = c + "Analytics"
print(d)

# RDD Creation and Transformation
lisin = [1, 2, 3, 4]
print("===== RAW LIST=====")
print(lisin)

rddin = sc.parallelize(lisin)
print("===== rddin LIST=====")
print(rddin.collect())

# Applying a transformation (map)
addin = rddin.map(lambda x: x + 2)
print("===== addin LIST=====")
print(addin.collect())

# Applying a transformation (filter)
filin = rddin.filter(lambda x: x > 2)
print("===== filin LIST=====")
print(filin.collect())

# Working with String RDDs
listr = ["zeyobron", "zeyo", "byte"]
print("===== RAW LIST=====")
print(listr)

rddstr = sc.parallelize(listr)
print("===== rddstr LIST=====")
print(rddstr.collect())

# Applying a transformation (map) to concatenate strings
conrdd = rddstr.map(lambda x: x + "Analytics")
print("===== conrdd LIST=====")
print(conrdd.collect())

# Applying a transformation (map) to replace substrings
reprdd = rddstr.map(lambda x: x.replace("zeyo", "tera"))
print("===== reprdd LIST=====")
print(reprdd.collect())

# Working with Nested Data (flatMap)
listrf = ["A~B", "C~D", "E~F"]
print("===== listrf LIST=====")
print(listrf)

flatrdd = sc.parallelize(listrf)
flatdata = flatrdd.flatMap(lambda x: x.split("~"))
print("===== flatdata LIST=====")
print(flatdata.collect())

# Stop the SparkContext
sc.stop()