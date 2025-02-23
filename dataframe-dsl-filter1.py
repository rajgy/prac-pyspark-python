# Import necessary libraries
from pyspark import SparkConf, SparkContext  # Spark configuration and context
from pyspark.sql import SparkSession  # Spark session for DataFrame operations
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

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("pyspark") \
    .getOrCreate()

# Print a message to indicate the program has started
print("STARTED=============")

#Reading Data
data = [
    ("00000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00005", "02-14-2011", 200, "Gymnastics", None, "cash")
]

# Converting data into dataframe
df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()

#Single filter
sincol = df.filter("category='Exercise'")
print("\n======SINGLE COL FILTER = category = 'Exercise'")
sincol.show()

#Multiple column filter using and operator
multicol = df.filter("category = 'Exercise' and spendby = 'cash'")
print("\n======Multi COL FILTER = category = 'Exercise' and spendby = 'cash'")
multicol.show()

#Multiple Column filter using or operator
multicolor = df.filter("category = 'Exercise' or spendby = 'cash'")
print("\n======Multi COL OR FILTER = category = 'Exercise' and spendby = 'cash'")
multicolor.show()

#Multiple Value Filter using in operator
multivalue = df.filter("category in ('Exercise', 'Gymnastics')")
print("\n======Multi VALUE FILTER = category = 'Exercise' and spendby = 'cash'")
multivalue.show()


#Filater usinf like operator
likeFilter = df.filter("product like 'filter' ")
print("\n======product like 'gymnastic'")
likeFilter.show()

#Filter using to find out the null value by using is operator
nullfilter = df.filter("product is null")
print("\n=====product is nul=====")
nullfilter.show()


#Filter is not null using is not operator
notnullfilter = df.filter("product is not null")
print("\n=====product is not nul=====")
notnullfilter.show()