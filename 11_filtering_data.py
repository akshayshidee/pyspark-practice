import pyspark
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("sparkexample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data=[("James","Bond","100",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",''),
      ("Tom Brand",None,"400",'M')] 
columns=["fname","lname","id","gender"]

df=spark.createDataFrame(data,schema=columns)

print("_____________________")
df.show()
print("_____________________")

df.filter(df.fname == "James").show()
df.filter(df.fname != "James").show()
df.filter((df.fname == "James") & (df.id == "100")).show()
df.filter((df.fname == "James" )| (df.id != "100")).show()

df.filter(df.id.between(100,300)).show()
df.filter(df.lname.contains("Bond")).show()
df.filter(df.fname.startswith("T")).show()
df.filter(df.fname.endswith("s")).show()
df.filter(df.lname.isNull()).show()
df.filter(df.lname.isNotNull()).show()

df.filter(df.fname.like("To%")).show()
df.filter(df.fname.rlike("(?i)^tom")).show()
df.filter(df.id.isin(["100","200"])).show()