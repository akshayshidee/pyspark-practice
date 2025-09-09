import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark=SparkSession.builder.appName("sparkexample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Create DataFrame1
simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.show()

# Create DataFrame2
simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]

columns2= ["employee_name","department","state","salary","age","bonus"]
df2 = spark.createDataFrame(data = simpleData2, schema = columns2)
df2.show()


###### union -- it display record combining duplicates.It combine based on column possition not name.
# spark deprecated unionAll from spark2.0 as it work similar to union

print("union:")
df.union(df2).show()

######### unionByName -- it used to union the two dataframe by name of the column. also We can guid it to 
# manage the missing or mismatching column
print("unionByName:")
df.unionByName(df2, allowMissingColumns=True).show()

##### to drop duplicates  use distinct or dropDuplicates
df.union(df2).distinct().show() 

df.union(df2).dropDuplicates().show()