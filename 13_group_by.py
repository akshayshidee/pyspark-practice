import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg

spark=SparkSession.builder.appName("sparkexample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# Data
simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

# Create DataFrame
schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)

print("_________________")
df.show()
print("___________________")

### basic group by operation with single column
print("sum of salary by department:")
df.groupBy(df["department"]).sum("salary").show()

print("avg of salary by department:")
df.groupBy(df["department"]).avg("salary").show()

print("count of department:")
df.groupBy(df["department"]).count().show()

print("min of salary by department:")
df.groupBy(df["department"]).min("salary").show()

print("max of salary by department:")
df.groupBy(df["department"]).max("salary").show()

###  group by operation with mutiple column 
df.groupBy("department","state") \
    .sum("salary","bonus") \
    .show()
##to apply mutiple aggregation we can use agg

df.groupBy(df["department"])\
    .agg(\
        sum("salary").alias("sum_salary"),\
        avg("salary").alias("avg_salary")\
    ).show()