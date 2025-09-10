import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark=SparkSession.builder.appName("sparkexample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)

print("_____________________________")
print("Exaple dataframe:")
df.show()
print("_____________________________")

###>>>>>>>> ranking functions
windowSpec=Window.partitionBy("department").orderBy(col("salary").desc())

### row_number
print("row_number:")
df.withColumn("row_nnumber", row_number().over(windowSpec)).show()

### rank
print("rank:")
df.withColumn("rank", rank().over(windowSpec)).show()

### dense_rank
print("dense_rank:")
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

### ntile
print("ntile:")
df.withColumn("ntile", ntile(2).over(windowSpec)).show()

### percent_rank
print("percent_rank:")
df.withColumn("percent_rank", percent_rank().over(windowSpec)).show()

####>>>>> Analytic function
### lag
print("lag:")
df.withColumn("lag", lag("salary",1,0).over(windowSpec)).show()

### lead
print("lead:")
df.withColumn("lead", lead("salary",1,0).over(windowSpec)).show()

### cume_dist
print("cume_dist:")
df.withColumn("cume_dist", cume_dist().over(windowSpec)).show()

######>> Aggregated functions
print("Aggregated functions:")
df.withColumn("sum", sum("salary").over(windowSpec))\
    .withColumn("avg", avg("salary").over(windowSpec))\
    .withColumn("min", min("salary").over(windowSpec))\
    .withColumn("max", max("salary").over(windowSpec))\
    .show()