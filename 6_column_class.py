import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, when,struct,lit


spark=SparkSession.builder.appName("sparkExample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


data=[("James",23),("Ann",40)]
df=spark.createDataFrame(data, schema=["name.fname","age"])

print("_____________________________")
df.show()
print("_____________________________")

##selecting one column from dataframe

df.select(df.age).show()
df.select(df["age"]).show() #best industry standard practice-- avoid confusion , error due to same python variable name, or space in column name
df.select(col("age")).show() #best industry standard practice-- avoid confusion , error due to same python variable name, or space in column name

##accessng column with dot
df.select(df["`name.fname`"]).show()

# ## column operation
data1=[(100,2,1),(200,3,4),(300,4,4)]
df1=spark.createDataFrame(data1).toDF("col1","col2","col3")


df1.select(df1.col1 + df1.col2).show()
df1.select(df1.col1 - df1.col2).show()
df1.select(df1.col1 * df1.col2).show()
df1.select(df1.col1 / df1.col2).show()
df1.select(df1.col1 % df1.col2).show()
df1.select(df1.col1 > df1.col2).show()
df1.select(df1.col1 < df1.col2).show()
df1.select(df1.col1 == df1.col2).show()

####common function with column
data=[("James","Bond","100",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",''),
      ("Tom Brand",None,"400",'M')] 
columns=["fname","lname","id","gender"]

df=spark.createDataFrame(data,columns)
print("_____________________________")
df.show()
print("_____________________________")

##giving temp name to column for further processing
df.select(df.fname.alias("first_name")).show()
df.select(df.lname.alias("last_name")).show()

##sorting operations
df.sort(df.fname.asc()).show()
df.sort(df.lname.desc()).show()
df.sort(df.lname.asc_nulls_first()).show()
df.sort(df.lname.asc_nulls_last()).show()
df.sort(df.lname.desc_nulls_first()).show()
df.sort(df.lname.desc_nulls_last()).show()

df.select(df.id.cast("int")).printSchema()

df.filter(df.id.between(100,300)).show()
df.filter(df.fname.contains("Cruise")).show()
df.filter(df.fname.startswith("T")).show()
df.filter(df.fname.endswith("Cruise")).show()
df.filter(df.lname.isNull()).show()
df.filter(df.lname.isNotNull()).show()
df.filter(df.fname.like("Tom%")).show()
df.filter(df.fname.rlike("(?i)^tom")).show()
df.filter(df.id.isin(["100","200"])).show()

##get substring from the column 
df.select(df.fname.substr(1,2).alias("substr")).show()

## when for situation like case stement in sql
df.select(df.fname,df.lname,df.gender
          ,when(df.gender=="M" ,"Male") \
          .when(df.gender=="F", "Female")\
            .when(df.gender==None , "")\
                .otherwise(df.gender).alias("new_gender")
          ).show()

## bitwise operation on the integer type column
data3=[(100,2,1),(200,3,4),(300,4,4)]
df3=spark.createDataFrame(data3).toDF("col1","col2","col3")
print("_____________________________")
df3.show()
print("_____________________________")

print("bitwiseAND")
df3.select(df3["col1"].bitwiseAND(df3["col2"])).show()
print("bitwiseOR")
df3.select(df3["col1"].bitwiseOR(df3["col2"])).show()
print("bitwiseXOR")
df3.select(df3["col1"].bitwiseXOR(df3["col2"])).show()

## eqNullSafe for equality test that is safe for null values
df4 = spark.createDataFrame([(None, None), (1, None), (None, 2), (2, 2)], ["a", "b"])
print("_____________________________")
df4.show()
print("_____________________________")

print("eqNullSafe for equality test that is safe for null values")
df4.select(df4["a"].eqNullSafe(df4["b"])).show()

## getfield return the field value specied inside a structure or in map as key
df5 = spark.createDataFrame([("James", "Bond")], ["fname", "lname"]) \
    .withColumn("full", struct("fname", "lname"))

print("_____________________________")
df5.show()
print("_____________________________")

df5.select(df5["full"].getField("lname")).show()

## withField Adds or replaces a field in a struct column
df5=df5.select(df5["full"].withField("age", lit(10)).alias("full_new"))
df5.show()

## dropField drop field in a struct column
df5=df5.select(df5["full_new"].dropFields("age").alias("full_new1"))
df5.show()

##getItem -Accesses an element by index (array) or key (map)
df6 = spark.createDataFrame([(["a", "b", "c"],)], ["letters"])
print("_____________________________")
df6.show()
print("_____________________________")

df6.select(df6["letters"].getItem(1)).show() #index start from 0

df7 = spark.createDataFrame([({"key1": "val1", "key2": "val2"},)], ["my_map"])
print("_____________________________")
df7.show()
print("_____________________________")

df7.select(df7["my_map"].getItem("key1")).show()

