import pyspark
from pyspark.sql import SparkSession

import os
print("JAVA_HOME:", os.environ.get("JAVA_HOME"))


spark = SparkSession.builder.appName('SparkByExamples.com').master("local[*]").getOrCreate()
# Set Spark log level
spark.sparkContext.setLogLevel("ERROR")


data = [("James","","Smith","36636","M",60000),
        ("Michael","Rose","","40288","M",70000),
        ("Robert","","Williams","42114","",400000),
        ("Maria","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns)

pandasDF=pysparkDF.toPandas()
print(pandasDF)
# pysparkDF.printSchema()
# pysparkDF.show()