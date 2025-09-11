import pyspark
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("sparkexample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", None),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", None),  \
    ("James", None, 3000),    \
    ("Scott", None, 3300),  \
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

df.fillna(value=0, subset=["salary"]).show()

df.na.fill({"department":"unknown", "salary":0}).show()