import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType

spark=SparkSession.builder.appName("sparkexample.com").master("local[*]").getOrCreate()

data=[("Finance", 10), ("Marketing", 20)]
rdd=spark.sparkContext.parallelize(data)

df=rdd.toDF() ###coverting RDD to dataframe deafualt column -1, -2 etc
print("_____________________________")
df.show()
print("_____________________________")

df_n=rdd.toDF(["dept_name", "dept_id"]) ###coverting RDD to dataframe with column info
print("_____________________________")
df_n.show()
print("_____________________________")
