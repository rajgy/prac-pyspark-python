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

#  full rdd operations

print("=====STARTED======")

a = 2
print(a)


b = a + 2
print(b)


c = "zeyobron"
print(c)


d = c + "Analytics"
print(d)



lisin = [ 1 , 2 , 3 , 4]
print()
print("===== RAW LIST=====")
print(lisin)


rddin = sc.parallelize(lisin)
print()
print("===== rddin LIST=====")
print(rddin.collect())




addin = rddin.map( lambda x : x  + 2 )
print()
print("===== addin LIST=====")
print(addin.collect())


filin = rddin.filter(lambda x : x > 2)
print()
print("===== filin LIST=====")
print(filin.collect())





listr =  [ "zeyobron" , "zeyo" , "byte" ]
print()
print("===== RAW LIST=====")
print(listr)






rddstr = sc.parallelize(listr)
print()
print("===== rddstr LIST=====")
print(rddstr.collect())







conrdd = rddstr.map(lambda   x  :  x  +  "Analytics")
print()
print("===== conrdd LIST=====")
print(conrdd.collect())




reprdd = rddstr.map(lambda x : x.replace("zeyo","tera"))
print()
print("===== reprdd LIST=====")
print(reprdd.collect())



listrf = [ "A~B" , "C~D" , "E~F" ]
print()
print("===== listrf LIST=====")
print(listrf)


flatrdd = sc.parallelize(listrf)


flatdata = flatrdd.flatMap(lambda x : x.split("~"))
print()
print("===== flatdata LIST=====")
print(flatdata.collect())