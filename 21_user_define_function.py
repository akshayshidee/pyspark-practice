import pyspark
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("sparkexample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

print("_____________________________")
print("Exaple dataframe:")
df.show()
print("_____________________________")

def convert_case(text):
    if text is None:
        return None
    resStr=""
    arr = text.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr 

convert_udf=udf(convert_case,StringType())

df2=df.withColumn("CurratedName", convert_udf(col("Name")))
df2.show()

####use it with sql like query
spark.udf.register("convertUDF", convert_case, StringType())
df.createOrReplaceTempView("user_data")
spark.sql("select Seqno,Name, convertUDF(Name) from user_data").show()

###### we can create the function with python decorator

@udf(returnType= StringType())
def to_upper(text):
    return text.upper() if text else None

df2=df.withColumn("upperName", to_upper("Name"))
print("we can create the function with python decorator")
df2.show()