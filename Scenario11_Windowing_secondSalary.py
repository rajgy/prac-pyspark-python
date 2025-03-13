# Import necessary libraries
from pyspark import SparkConf, SparkContext  # Spark configuration and context
from pyspark.sql import SparkSession         # Spark session for DataFrame operations
import os                                    # For setting environment variables
import sys                                   # For accessing system-specific parameters
from pyspark.sql.functions import *          # For using SQL functions like `col`, `filter`, etc.
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window

# Step 1: Set up environment variables for PySpark
python_path = sys.executable                          # Get the current Python interpreter path
os.environ['PYSPARK_PYTHON'] = python_path            # Tell PySpark to use this Python interpreter
os.environ['HADOOP_HOME'] = "hadoop"                   # Set Hadoop home directory
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


# -----------------------------------------------
# Step 1: Create a List of Tuples (Sample Data)
# -----------------------------------------------

# Each tuple contains two values:
# First value = Department Name (e.g., 'DEPT1')
# Second value = Salary (e.g., 1000)
data = [
    ("DEPT1", 1000),
    ("DEPT1", 700),
    ("DEPT1", 500),
    ("DEPT2", 400),
    ("DEPT2", 200),
    ("DEPT3", 500),
    ("DEPT3", 200)
]

# -----------------------------------------------
# Step 2: Define Column Names for DataFrame
# -----------------------------------------------

# List of column names corresponding to each element in the tuple
columns = ["dept", "salary"]

# -----------------------------------------------
# Step 3: Create DataFrame from List of Tuples
# -----------------------------------------------

# Convert the list of tuples into a PySpark DataFrame
# This DataFrame will have two columns: 'dept' and 'salary'
df = spark.createDataFrame(data, columns)

# Show the content of the DataFrame in a table format
df.show()



# -----------------------------------------------
# Step 4: Create a Window Specification
# -----------------------------------------------

# Window functions allow you to perform operations across a group of rows
# that are related to the current row (like ranking, row number, cumulative sum).

# partitionBy("dept") means:
# - Group the data by 'dept' column.
# - Each department's data will be considered as one group for window functions.

# orderBy(col("salary").desc()) means:
# - Within each 'dept' group, order the rows by 'salary' in descending order.
# - So highest salary comes first in each department group.

# Together, this Window specification allows us to perform operations
# (like ranking, row number, etc.) within each department and ordered by salary.

# Example operations that can use this window: row_number(), rank(), dense_rank(), sum(), avg(), etc.

# Import Window and col for use
from pyspark.sql.window import Window
from pyspark.sql.functions import col

# Define the window
deptwindow = Window.partitionBy("dept").orderBy(col("salary").desc())

#==STEP 2===APPLYING WITH WINDOW ON DATAFRAME TO DENSE RANK
drank = df.withColumn("drank",dense_rank().over(deptwindow))
drank.show()

#===STEP 3=== FILTER RANK =2
filrank = drank.filter("drank=2")
filrank.show()

finaldf = filrank.drop("drank")
finaldf.show()