import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark=SparkSession.builder.appName("sparkexample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#Create spark session
data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]
df = spark.createDataFrame(data = data, schema = columns)

print("_________________")
df.show()
print("___________________")

print("pivoted table:")
df.groupBy("Product").pivot("Country").sum("Amount").show()

## we can pass pivote column also if we know for faster performance

print("performance optimized pivoted table:")
country=["Canada","China","Mexico","USA"]
pivotedDF=df.groupBy("Product").pivot("Country",country).sum("Amount")
pivotedDF.show()

##unpivot

print("unpivoted table:")
unpivotExpr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
# print(unpivotExpr)

unPivotDF = pivotedDF.select("Product", expr(unpivotExpr)) \
    .where("Total is not null")
    
unPivotDF.show()