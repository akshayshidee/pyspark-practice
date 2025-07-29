import pyspark
from  pyspark.sql import SparkSession

spark=SparkSession.builder.appName('sparkexample.com').master("local[*]").getOrCreate()

emptyRDD=spark.sparkContext.parallelize([])
print("________________________________")
print(emptyRDD)
print("________________________________")

emptyRDD2=spark.sparkContext.emptyRDD()
print("________________________________")
print(emptyRDD2)
print("________________________________")