import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType    

spark=SparkSession.builder.appName("sparkExample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data=[("akshay","shinde",["python","sql","spark"],28),("harh","rana",["python","spark"],26)
      ,("raj","patel",["python","sql"],29)]
df=spark.createDataFrame(data,schema=["fName","lName","lang","age"])

print("________________________")
print("original Dataframe: ")
df.show()
print("________________________")


## how to select the single column
print("select the single column: ")
df.select("fName").show()
df.select(df.fName).show()
df.select(df["fName"]).show() #best industry standard practice-- avoid confusion , error due to same python variable name, or space in column name
df.select(col("fName")).show()#best industry standard practice-- avoid confusion , error due to same python variable name, or space in column name

## how to select the multiple column
print("select the multiple column: ")
df.select("fName","lName").show()
df.select(df.fName,df.lName).show()
df.select(df["fName"],df["lName"]).show()#best industry standard practice-- avoid confusion , error due to same python variable name, or space in column name
df.select(col("fName"),col("lName")).show()#best industry standard practice-- avoid confusion , error due to same python variable name, or space in column name

## select column with regex type column name match
print("select column with regex type column name match: ")
df.select(df.colRegex("`^.*name*`")).show()

##select column from specified list
print("select column from specified list: ")
columns=["fName","lName","lang"]

df.select(*columns).show()
print(df.columns)
df.select([col for col in df.columns]).show()

## select column by index
print("select column by index: ")
df.select(df.columns[:3]).show()


## select all the columns
print("select all the columns: ")
df.select("*").show()

## selecting complex data  column
data = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]
    
schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])
df2 = spark.createDataFrame(data = data, schema = schema)
print("________________________")
print("original Dataframe: ")
df2.show()
print("________________________")

print("selecting complex data  column: ")
df2.select("name").show()
df2.select("name.firstname").show()
df2.select("name.*").show()