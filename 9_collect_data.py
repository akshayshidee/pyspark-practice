import pyspark
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("sparkexample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data=[("akshay","shinde",["python","sql","spark"],28),("harh","rana",["python","spark"],26)
      ,("raj","patel",["python","sql"],29)]
df=spark.createDataFrame(data,schema=["fName","lName","lang","age"])

print("________________________")
df.show()
print("________________________")

for row in df.collect():
    print(row['fName'])
    
print(df.collect()[0][0])