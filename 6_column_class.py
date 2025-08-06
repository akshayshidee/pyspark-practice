import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark=SparkSession.builder.appName("sparkExample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# data=[("James",23),("Ann",40)]
# df=spark.createDataFrame(data, schema=["name.fname","age"])

# print("_____________________________")
# df.show()
# print("_____________________________")

# ##selecting one column from dataframe

# df.select(df.age).show()
# df.select(df["age"]).show()
# df.select(col("age")).show()

# ##accessng column with dot
# df.select(df["`name.fname`"]).show()

## column operation
data=[(100,2,1),(200,3,4),(300,4,4)]
df1=spark.createDataFrame(data).toDF("col1","col2","col3")


df1.select(df1.col1 + df1.col2).show()
df1.select(df1.col1 - df1.col2).show()
df1.select(df1.col1 * df1.col2).show()
df1.select(df1.col1 / df1.col2).show()
df1.select(df1.col1 % df1.col2).show()
df1.select(df1.col1 > df1.col2).show()
df1.select(df1.col1 < df1.col2).show()
df1.select(df1.col1 == df1.col2).show()