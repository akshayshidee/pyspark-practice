import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark=SparkSession.builder.appName("sparkExample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# data=[("James",23),("Ann",40)]
# df=spark.createDataFrame(data, schema=["name.fname","age"])

# print("_____________________________")
# df.show()
# print("_____________________________")

# ##selecting one column from dataframe

# df.select(df.age).show()
# df.select(df["age"]).show()
# df.select(col("age")).show()

# ##accessng column with dot
# df.select(df["`name.fname`"]).show()

# ## column operation
# data=[(100,2,1),(200,3,4),(300,4,4)]
# df1=spark.createDataFrame(data).toDF("col1","col2","col3")


# df1.select(df1.col1 + df1.col2).show()
# df1.select(df1.col1 - df1.col2).show()
# df1.select(df1.col1 * df1.col2).show()
# df1.select(df1.col1 / df1.col2).show()
# df1.select(df1.col1 % df1.col2).show()
# df1.select(df1.col1 > df1.col2).show()
# df1.select(df1.col1 < df1.col2).show()
# df1.select(df1.col1 == df1.col2).show()

#####common function with column
data=[("James","Bond","100",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",''),
      ("Tom Brand",None,"400",'M')] 
columns=["fname","lname","id","gender"]

df=spark.createDataFrame(data,columns)
print("_____________________________")
df.show()
print("_____________________________")

# df.select(df.fname.alias("first_name")).show()
# df.select(df.lname.alias("last_name")).show()

# df.sort(df.fname.asc()).show()
# df.sort(df.lname.desc()).show()

# df.select(df.id.cast("int")).printSchema()

# df.filter(df.id.between(100,300)).show()
# df.filter(df.fname.contains("Cruise")).show()
# df.filter(df.fname.startswith("T")).show()
# df.filter(df.fname.endswith("Cruise")).show()
# df.filter(df.lname.isNull()).show()
# df.filter(df.lname.isNotNull()).show()
# df.filter(df.fname.like("Tom%")).show()
# df.filter(df.fname.rlike("(?i)^tom")).show()
# df.filter(df.id.isin(["100","200"])).show()

# df.select(df.fname.substr(1,2).alias("substr")).show()

from pyspark.sql.functions import when
df.select(df.fname,df.lname,df.gender
          ,when(df.gender=="M" ,"Male") \
          .when(df.gender=="F", "Female")\
            .when(df.gender==None , "")\
                .otherwise(df.gender).alias("new_gender")
          ).show()