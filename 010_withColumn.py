import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, struct


spark=SparkSession.builder.appName("sparkExample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]

df=spark.createDataFrame(data,schema=columns)

print("________________________")
df.show()
print("________________________")

## converting data type of the existing column
df.withColumn("salary",col("salary").cast("Integer")).show()

## change value of the existing column
df.withColumn("salary", col("salary")*100).show()

##add the column with constant value
df.withColumn("country", lit("InD")).show()

## rename the existing column
df.withColumnRenamed('gender', 'sex').show()

##remove the column
df.drop("salary").show()

## complex data type
df1 = spark.createDataFrame([("James", "Bond", "007")], ["fname", "mname","lname"]) \
    .withColumn("full", struct("fname","mname", "lname"))
    
print("________________________")
df1.show()
print("________________________")    

## adding column inside the sturcture
df1.withColumn("full_new", col("full").withField("age", lit(10))).show()

##dropping column from the structure
df1.withColumn("full" , col("full").dropFields("lname")).show()
