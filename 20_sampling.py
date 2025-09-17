import pyspark
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("sparkexample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


df = spark.range(100)

print("_____________________________")
print("Exaple dataframe:")
df.show()
print("_____________________________")

print("sample(withReplcement,fraction,seed):")
df.sample(False,0.06,seed=123).show()
print("_____________________________")

print("sampleBy(col,fraction,seed):")
df2=df.select((df.id % 3).alias("key"))
df2.sampleBy("key",{0:0.1, 1:0.2},seed=123).show()
print("_____________________________")

########>> rdd sampling
rdd = spark.sparkContext.range(0,100)

print("sample RDD:")
print(rdd.collect())
print("_____________________________")

print("rdd sample:")
print(rdd.sample(False,0.1,0).collect())
print("_____________________________")

print("rdd takeSample:")
print(rdd.takeSample(True,5,123))