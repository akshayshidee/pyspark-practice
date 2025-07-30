import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType, IntegerType

spark=SparkSession.builder.appName("sparkExample.com").master("local[*]").getOrCreate()
# Set Spark log level
spark.sparkContext.setLogLevel("ERROR")

data=[("Finance", 10), ("Marketing", 20)]

schema= StructType([
    StructField('DeptName',StringType(), True),
    StructField('Quntity',IntegerType(),True)
])

pyspark_df=spark.createDataFrame(data,schema)
print("_____________________________")
pyspark_df.show()
print("_____________________________")

pandas_df=pyspark_df.toPandas()
print("_____________________________")
print(pandas_df)
print("_____________________________")

#######converting nested structutre dataframe to pandas
dataStruct = [(("Akshay","","Shinde"),"36636","M","3000"), \
      (("Kajal","Shinde",""),"40288","F","4000"), \
      (("Robert","","Williams"),"42114","M","4000"), \
      (("Raj","Rajan","Malotra"),"39192","F","4000"), \
      (("Jen","Mary","Brown"),"","F","-1") \
]

schemaStruct = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
          StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', StringType(), True)
         ])

pyspark_nested_df = spark.createDataFrame(data=dataStruct, schema = schemaStruct)
print("_____________________________")
pyspark_nested_df.show()
print("_____________________________")

pandas_nested_df=pyspark_nested_df.toPandas()
print("_____________________________")
print(pandas_nested_df)
print("_____________________________")