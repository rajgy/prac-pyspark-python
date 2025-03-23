import urllib.request
import ssl


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


urldata = (

    urllib.request
    .urlopen("https://randomuser.me/api/0.8/?results=10",context=ssl._create_unverified_context())
    .read()
    .decode("utf-8")


)

print(urldata)


rdd = sc.parallelize([urldata])

df = spark.read.json(rdd)

df.show()
df.printSchema()



explodedf = df.withColumn("results",expr("explode(results)"))
explodedf.show()

explodedf.printSchema()



finalexplode =  explodedf.select(

    "nationality",
    "results.user.cell",
    "results.user.dob",
    "results.user.email",
    "results.user.gender",
    "results.user.location.city",
    "results.user.location.state",
    "results.user.location.street",
    "results.user.location.zip",
    "results.user.md5",
    "results.user.name.first",
    "results.user.name.last",
    "results.user.name.title",
    "results.user.password",
    "results.user.phone",
    "results.user.picture.large",
    "results.user.picture.medium",
    "results.user.picture.thumbnail",
    "results.user.registered",
    "results.user.salt",
    "results.user.sha1",
    "results.user.sha256",
    "results.user.username",
    "seed",
    "version"

)

finalexplode.show()

finalexplode.printSchema()
