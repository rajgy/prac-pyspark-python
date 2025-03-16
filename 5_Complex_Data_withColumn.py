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




# 🔴WITHCOLUMN IMAGE EXAMPLE


data="""

{
	"id": "000",
	"type": "donut",
	"name": "Non cream",
	"image": {
		"url": "images/0001.jpg",
		"width": 200,
		"height": 200
	},
	"thumbnail": {
		"url": "images/thumbnails/0001.jpg",
		"width": 33,
		"height": 33
	}
}


"""

rdd = sc.parallelize([data])

df = spark.read.option("multiline","true").json(rdd)


df.show()

df.printSchema()




withflat = (

    df.withColumn( "i_height" , expr("image.height") )
    .withColumn( "i_url" , expr("image.url") )
    .withColumn( "i_width" , expr("image.width") )
    .withColumn( "t_height" , expr("thumbnail.height") )
    .withColumn( "t_url" , expr("thumbnail.url") )
    .withColumn( "t_width" , expr("thumbnail.width") )
    .drop("image","thumbnail")




)

withflat.show()

withflat.printSchema()