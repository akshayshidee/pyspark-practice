import pyspark
from  pyspark.sql import SparkSession

spark=SparkSession.builder.appName('sparkexample.com').master("local[*]").getOrCreate()
# Set Spark log level
spark.sparkContext.setLogLevel("ERROR")

emptyRDD=spark.sparkContext.parallelize([])
print("________________________________")
print(emptyRDD)
print("________________________________")

emptyRDD2=spark.sparkContext.emptyRDD()
print("________________________________")
print(emptyRDD2)
print("________________________________")