import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark=SparkSession.builder.appName("sparkExample.com").master("local[*]").getOrCreate()
#set spark log level
spark.sparkContext.setLogLevel("ERROR")

# Define schema using StructType with various field types
schemaStruct = StructType([
    StructField("string_col", StringType(), True),
    StructField("int_col", IntegerType(), True),
    StructField("long_col", LongType(), True),
    StructField("double_col", DoubleType(), True),
    StructField("float_col", FloatType(), True),
    StructField("boolean_col", BooleanType(), True),
    StructField("date_col", DateType(), True),
    StructField("timestamp_col", TimestampType(), True)
])

dataStruct = [
    ("Akshay", 25, 10000000000, 75.5, 45.0, True, 
     datetime.date(2023, 12, 25),
    datetime.datetime(2023, 12, 25, 15, 30, 0))
]

df=spark.createDataFrame(data=dataStruct,schema=schemaStruct)
print("_____________________________")
df.show()
print("_____________________________")

#complex struct types
schemaComplexStruct=StructType([
    StructField("arry_col", ArrayType(StringType()), True),
    StructField("map_col", MapType(StringType(), IntegerType()), True),
    StructField("nested_struct_col", StructType([
        StructField("nested_int", IntegerType(), True),
        StructField("nested_str", StringType(), True)
    ]), True)
])

dataComplexStruct = [
    (["spark", "pyspark"], 
     {"a": 1, "b": 2}, 
     (10, "nested-value"))
]

df=spark.createDataFrame(data=dataComplexStruct,schema=schemaComplexStruct)
print("_____________________________")
df.show()
print("_____________________________")