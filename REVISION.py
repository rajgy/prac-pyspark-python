# Import the necessary libraries for Spark


from pyspark import SparkConf, SparkContext  # SparkConf and SparkContext are used to configure and initialize Spark
from pyspark.sql import SparkSession         # SparkSession is used for working with DataFrames and SQL in Spark
import os                                    # os is used to interact with the operating system (e.g., setting environment variables)
import sys                                   # sys is used to access system-specific parameters (e.g., Python interpreter path)

from pyspark.sql.functions import *

from Scenario11_Windowing_secondSalary import deptwindow

# Step 1: Set up environment variables for PySpark
python_path = sys.executable  # Get the path of the current Python interpreter
os.environ['PYSPARK_PYTHON'] = python_path  # Tell PySpark to use this Python interpreter
os.environ['HADOOP_HOME'] = r"D:\BigData\bigdata\bath42\Spark\pyspark\pyspark\hadoop"  # Set the Hadoop home directory (required for Windows)
os.environ['JAVA_HOME'] = r'C:\Users\cogni\.jdks\corretto-1.8.0_442'  # Set the Java home directory (Spark requires Java)

# Step 2: Configure Spark
conf = SparkConf().setAppName("pyspark").setMaster("local[*]")  # Create a Spark configuration
# - setAppName("pyspark"): Give a name to the Spark application
# - setMaster("local[*]"): Run Spark locally using all available CPU cores

sc = SparkContext(conf=conf)  # Initialize the SparkContext using the configuration

# Initialize SparkSession
spark = SparkSession.builder.appName("Revision of Pyspark Code").getOrCreate() # Set the application name # Create a SparkSession or reuse an existing one

# Print a message to indicate the program has started
print("STARTED=============")

# Load the file into an RDD (Resilient Distributed Dataset)
# - sc.textFile(): Reads a text file and returns an RDD of strings (each line is a string)
# - r"D:\BigData\bigdata\bath42\Spark\pyspark\pyspark\file1.txt": The absolute path to the file (use raw string to avoid escape character issues)
filerdd = sc.textFile(r"D:\BigData\bigdata\bath42\Spark\pyspark\pyspark\file1.txt")

# Filter the RDD to include only lines containing the word 'Gymnastics'
# - filter(lambda x: 'Gymnastics' in x): Keeps only lines where 'Gymnastics' is present
gymrdd = filerdd.filter(lambda x: 'Gymnastics' in x)

# Print a message to indicate the filtering step is complete
print("\n====== FILTER GYMNASTICS=====\n")
print()

# Split each line of the RDD by commas
# - map(lambda x: x.split(",")): Splits each line into a list of strings using comma as the delimiter
mapsplit = gymrdd.map(lambda x: x.split(","))

# Import namedtuple from the collections module
# - namedtuple: A factory function for creating tuple subclasses with named fields
from collections import namedtuple

# Define a namedtuple schema for the data
# - columns: The name of the namedtuple
# - ['txnno', 'txndate', 'custno', 'amount', 'category', 'product', 'city', 'state', 'spendby']: The field names
columns = namedtuple('columns', ['txnno', 'txndate', 'custno', 'amount', 'category', 'product', 'city', 'state', 'spendby'])

# Apply the schema to the RDD
# - map(lambda x: columns(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8])): Converts each list into a namedtuple
schemardd = mapsplit.map(lambda x: columns(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8]))

# Filter the RDD to include only rows where 'product' contains 'Gymnastics'
# - filter(lambda x: 'Gymnastics' in x.product): Keeps only rows where 'Gymnastics' is in the 'product' field
prodfilted = schemardd.filter(lambda x: 'Gymnastics' in x.product)

# Print each row of the filtered RDD
# - foreach(print): Applies the `print` function to each element of the RDD
prodfilted.foreach(print)


# Creating Dataframe
schemadf = prodfilted.toDF()

print("\n=====SCHEMA DF======\n")
schemadf.show(5)

# Reading CSV file from given local location of Laptop
csvdf = spark.read.format("csv").option("header","true").load(r"D:\BigData\bigdata\bath42\Spark\pyspark\pyspark\file3.txt")
print("\n=====csvdf DF======\n")
csvdf.show(5)

#Reading the JSON file format using the SRFOL formala from the local location of laptop
jsondf = spark.read.format("json").load(r"D:\BigData\bigdata\bath42\Spark\pyspark\pyspark\file4.json").select('txnno','txndate','custno','amount','category','product','city','state','spendby')
print("\n=====jsondf DF======\n")
jsondf.show(5)

# Reading Parquet file Format Using SRFOL formula
parquetdf = spark.read.load(r"D:\BigData\bigdata\bath42\Spark\pyspark\pyspark\file5.parquet")
print("\n=====parquetdf DF======\n")
parquetdf.show(5)

# Merging all above reading file USing UNION
uniondf = schemadf.union(csvdf).union(jsondf).union(parquetdf)
print("\n=====uniondf DF======\n")
uniondf.show(5)




#Processing/ perforing transformation on uniondf dataframe using withColumn
procdf =(
    uniondf.withColumn("txndate" , expr("split(txndate,'-')[2]"))
    .withColumnRenamed("txndate", "year")
    .withColumn("status",expr("case when spendby='cash' then 0 else 1 end"))
    .filter("txnno>50000")

)

print("\n=====procdf DF======\n")
procdf.show(5)


# Processing or Transformation data using Aggregate i.e. agg
aggdf = procdf.groupBy("category").agg(sum("amount").alias('total'))
print("\n=======aggdf of procdf======\n")
aggdf.show()

# Creating the List of Tuples

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

cust = spark.createDataFrame(data4, ["id","name"]).coalesce(1)
print("\n=======custumer dataframe=====\n")
cust.show()

#Creatinf the list of tuples of product
data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]
prod = spark.createDataFrame(data3,["id","product"]).coalesce(1)
print("\n=====product dataframe======\n")
prod.show()

# using inner join
inner = cust.join(prod,["id"],"inner")
print("\n=====Inner Join between customer and product dataframe====\n")
inner.show()

#Using left join
left=cust.join(prod,["id"],"left")
print("\n======Left Join between customer and product dataframe======\n")
left.show()

#Using right join
right=cust.join(prod,["id"],"right")
print("\n=====Right join between customer and product dataframe=======\n")
right.show()

#Using full join
full=cust.join(prod,["id"],"full")
print("\n====Full Join between Customer and Product===\n")
full.show()

#Using anti join
anti=cust.join(prod,["id"],"left_anti")
print("\n====anti left join====\n")
anti.show()

#Using cross join
cross=cust.crossJoin(prod)
print("\n=====crossJoin====\n")
cross.show()

#creating list of tuples of dept1 salary
data = [
    ("DEPT1", 1000),
    ("DEPT1", 700),
    ("DEPT1", 500),
    ("DEPT2", 400),
    ("DEPT2", 200),
    ("DEPT3", 500),
    ("DEPT3", 200)]
df = spark.createDataFrame(data,["dept","salary"])
print("\n====Reading the dataframe of dept=====\n")
df.show()



##🔴🔴🔴🔴🔴🔴 STEP 1 --- CREATE THE WINDOW
from pyspark.sql.window import Window

deptwindow=Window.partitionBy("dept").orderBy(col("salary").desc())


##🔴🔴🔴🔴🔴🔴 STEP 2 --- APPLYING WITH WINDOW  ON DATAFRAME TO DENSE RANK
drank = df.withColumn("drank",dense_rank().over(deptwindow))
drank.show()

##🔴🔴🔴🔴🔴🔴 STEP 3  -- FILTER RANK =2
filrank = drank.filter("drank=2")
filrank.show()

filrank.drop("drank").show()