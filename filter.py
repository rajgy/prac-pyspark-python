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


lisin= [ 1 , 2 , 3 , 4]

print()
print("======== RAW LIST========")
print(lisin)


rddin = sc.parallelize(lisin)
print()
print("======== RDD LIST========")
print(rddin.collect())



fillin = rddin.filter(lambda x : x > 2)
print()
print("======== fillin LIST========")
print(fillin.collect())