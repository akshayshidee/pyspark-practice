import pyspark
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("sparkExample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data=[("James","Bond","100",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",''),
      ("Tom Brand",None,"400",'M')] 
columns=["fname","lname","id","gender"]

df=spark.createDataFrame(data,columns)
print("_____________________________")
df.show()
print("_____________________________")

##bsaic sort operation and default is asc
print("bsaic sort operation and default is asc")
df.sort(df["fname"]).show()
df.orderBy(df["fname"]).show()

##basic sort with keyword argument to specify the sort order
print("basic sort with keyword argument to specify the sort order")
df.sort(df["fname"], ascending=[True]).show()
df.sort(df["fname"], ascending=[False]).show()

##basic sort with asc() and desc() method to specify the sort order
print("basic sort with asc() and desc() method to specify the sort order")
df.sort(df["fname"].asc()).show()
df.sort(df["fname"].desc()).show()


## more sorting methods 
print("more sorting methods ")
df.sort(df["lname"].asc_nulls_first()).show()
df.sort(df["lname"].asc_nulls_last()).show()
df.sort(df["lname"].desc_nulls_first()).show()
df.sort(df["lname"].desc_nulls_last()).show()


