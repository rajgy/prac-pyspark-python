
# Import necessary libraries
from pyspark import SparkConf, SparkContext  # Spark configuration and context
# Import necessary libraries

from pyspark import SparkConf, SparkContext  # Spark configuration and context
from pyspark.sql import SparkSession  # Spark session for DataFrame operations
import os  # For setting environment variables
import sys  # For accessing system-specific parameters
from pyspark.sql.functions import *


# Step 1: Set up environment variables for PySpark
python_path = sys.executable  # Get the current Python interpreter path
os.environ['PYSPARK_PYTHON'] = python_path  # Tell PySpark to use this Python interpreter
os.environ['HADOOP_HOME'] = "hadoop"  # Set Hadoop home directory
os.environ['JAVA_HOME'] = r'C:\Users\ghimi\.jdks\corretto-1.8.0_442'  # Set Java home directory

# Step 2: Configure Spark
conf = SparkConf().setAppName("pyspark").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("pyspark") \
    .getOrCreate()

# Print a message to indicate the program has started
print("STARTED=============")




source_rdd = spark.sparkContext.parallelize([
    (1, "A"),
    (2, "B"),
    (3, "C"),
    (4, "D")
],1)

target_rdd = spark.sparkContext.parallelize([
    (1, "A"),
    (2, "B"),
    (4, "X"),
    (5, "F")
],2)

# Convert RDDs to DataFrames using toDF()
df1 = source_rdd.toDF(["id", "name"])
df2 = target_rdd.toDF(["id", "name1"])

# Show the DataFrames
df1.show()
df2.show()

print("===FULL JOIN====")


fulljoin = df1.join (df2, ["id"] , "full")
fulljoin.show()


match = fulljoin.withColumn("comment",expr("""

                                  case
                                  when  name=name1  then 'match'
                                  else 'mismatch'
                                  end



                        """))

match.show()



filterdf = match.filter(" comment ='mismatch' ")
filterdf.show()



finaldf = filterdf.withColumn("comment",expr("""

                                       case
                                       when name1 is null then 'New in Source'
                                       when name  is null then 'New in Target'
                                       else comment
                                       end

                                    """))


finaldf.show()


finalfinaldf = finaldf.drop("name","name1")
finalfinaldf.show()