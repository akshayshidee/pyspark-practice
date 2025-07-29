import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType

spark=SparkSession.builder.appName("sparkexample.com").master("local[*]").getOrCreate()

##### empty dataframe without schema
df_empty=spark.createDataFrame([],StructType([]))
print("_____________________________")
print(df_empty)
print("_____________________________")

##### empty dataframe with schema
schema= StructType([
    StructField('FirstName',StringType(), True),
    StructField('MiddleName',StringType(),True),
    StructField('LastName',StringType(),True)
])
df_empty2=spark.createDataFrame([],schema)
print("_____________________________")
print(df_empty2)
print("_____________________________")

######empty dataframe empty RDD and schema
emptyRDD=spark.sparkContext.emptyRDD()
df_empty3=spark.createDataFrame(emptyRDD,schema)
print("_____________________________")
print(df_empty3)
print("_____________________________")
