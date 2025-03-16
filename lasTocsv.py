# Import necessary libraries
from pyspark import SparkConf, SparkContext  # Spark configuration and context
from pyspark.sql import SparkSession         # Spark session for DataFrame operations
import os                                    # For setting environment variables
import laspy                                 # For reading LiDAR (.las) files
import sys                                   # For accessing system-specific parameters
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType  # For defining schema

# Step 1: Set up environment variables for PySpark
# This ensures PySpark uses the correct Python and Java paths
python_path = sys.executable                          # Get the current Python interpreter path
os.environ['PYSPARK_PYTHON'] = python_path            # Tell PySpark to use this Python interpreter
os.environ['HADOOP_HOME'] = r'D:\BigData\hadoop'      # Set Hadoop home directory
os.environ['JAVA_HOME'] = r'C:\Users\cogni\.jdks\corretto-1.8.0_442'  # Set Java home directory

# Step 2: Configure Spark
# Set up Spark configuration and initialize SparkContext
conf = SparkConf().setAppName("pyspark").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Initialize SparkSession
# SparkSession is the entry point for working with DataFrames in PySpark
spark = SparkSession.builder \
    .appName("pyspark") \
    .getOrCreate()

# Print a message to indicate the program has started
print("STARTED=============")

# Step 3: Read and process LiDAR (.las) data
# Load a LiDAR (.las) file using laspy
las_file_path = r'D:\BigData\bigdata\bath42\Spark\pyspark\pyspark\013.las'
las_file = laspy.read(las_file_path)




# Step 2: Extract attributes from the .las file
# Common attributes: X, Y, Z, classification, intensity, etc.
x = las_file.x
y = las_file.y
z = las_file.z
classification = las_file.classification
intensity = las_file.intensity

# Convert float64 values to Python native float
x = [float(val) for val in x]
y = [float(val) for val in y]
z = [float(val) for val in z]

# Convert uint8 values to Python native int
classification = [int(val) for val in classification]
intensity = [int(val) for val in intensity]

# Step 3: Create a PySpark DataFrame
# Combine the attributes into a list of tuples
las_data = list(zip(x, y, z, classification, intensity))

# Define the schema for the DataFrame
schema = StructType([
    StructField("X", FloatType(), nullable=False),
    StructField("Y", FloatType(), nullable=False),
    StructField("Z", FloatType(), nullable=False),
    StructField("Classification", IntegerType(), nullable=False),
    StructField("Intensity", IntegerType(), nullable=False)
])

# Create a PySpark DataFrame with the defined schema
las_df = spark.createDataFrame(las_data, schema=schema)

# Step 4: Display the DataFrame in the console
print("\nDisplaying LAS Data:\n")
las_df.show(5)
print("Count the total point:",las_df.count())

# filter by classification

ground_df = las_df.filter(col("classification")==2)
ground_df.show(30)
print("Count the point of ground:",ground_df.count())


# filter the ground data according to intensity value
intensity_df = ground_df.filter("Intensity >= 30000 and Intensity <= 45000")
intensity_df.show(10)
print("\n Count the filter Intensity Point:", intensity_df.count())

# filter the ground data according to intensity value
lessintensity_df = ground_df.filter("Intensity <= 30000")
lessintensity_df.show(10)
print("\n Count the filter Less Intensity Point:", lessintensity_df.count())


# Calculate the area covered by the points
x_range = las_df.select(max("X") - min("X")).collect()[0][0]
y_range = las_df.select(max("Y") - min("Y")).collect()[0][0]
area = x_range * y_range

# Calculate point density
point_density = las_df.count() / area
print(f"Point_Density in Given Area: {area}, PPM: {point_density}")

# # Extract X, Y, Z coordinates from the LiDAR file
# x = las_file.x
# y = las_file.y
# z = las_file.z
#
# # Convert float64 values to Python native float
# x = [float(val) for val in x]
# y = [float(val) for val in y]
# z = [float(val) for val in z]
#
# # Convert LiDAR data to a list of tuples
# lidar_data = list(zip(x, y, z))
#
# # Define the schema for the DataFrame
# schema = StructType([
#     StructField("X", FloatType(), nullable=False),
#     StructField("Y", FloatType(), nullable=False),
#     StructField("Z", FloatType(), nullable=False)
# ])
#
# # Create a PySpark DataFrame with the defined schema
# lidar_df = spark.createDataFrame(lidar_data, schema=schema)
#
# # Show the first few rows of the LiDAR DataFrame
# print("Displaying LiDAR Data:")
# lidar_df.show(5)
#
# num_points = lidar_df.count()
# print(f"Total number of points in the DataFrame: {num_points}")
#
# # Step 1: Filter elevation data for 1-meter intervals
# # Assuming `lidar_df` is the DataFrame containing X, Y, Z coordinates
# # Round the Z values to the nearest integer
#
# df = lidar_df.withColumn("Z", round(col("Z")))
# df.show(5)

# # Step 2: Filter the data to include only 1-meter intervals
# # Group by the rounded Z values and show the filtered data
# filtered_df =df.groupBy("Z_rounded").count().orderBy("Z_rounded")
#
# # Show the filtered DataFrame in the console
# print("Filtered LiDAR Data (1-Meter Intervals):")
# filtered_df.show()


# # Step 4: Save the LiDAR DataFrame to a CSV file (optional)
# csv_output_path = r'D:\BigData\writeData\lidar_data.csv'
# lidar_df.write.format("csv").mode("overwrite").save(csv_output_path)
# print(f"LiDAR data has been saved as CSV at: {csv_output_path}")

# Print a message to indicate the program has completed
print("\n======PROGRAM COMPLETED SUCCESSFULLY====\n")