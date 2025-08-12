import pyspark
from pyspark.sql import SparkSession, Row

spark=SparkSession.builder.appName("sparkexample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#creating row object
row=Row("akshay",28)
print(row[0])

##Using named argument
row=Row(name="akshay", age=29)
print(row.name)

##creating custom row like object
Person=Row("name", "age")
p1=Person("akshay", 28)
p2=Person("kajal",25)
print(p1.name, p2.age)

##row with RDD

data=[Row(name="akshay", lang=["python","sql","spark"], age=28),
      Row(name="kajal", lang=["python","spark"], age=25)]
rdd=spark.sparkContext.parallelize(data)
for row in rdd.collect():
    print(row.name, row.lang)
    
##row with dataframe
df=spark.createDataFrame(data)
df.show()
