# Pivot the Occupation column in OCCUPATIONS so that each Name is sorted alphabetically 
# and displayed underneath its corresponding Occupation. The output should consist 
# of four columns (Doctor, Professor, Singer, and Actor) in that specific order,
# with their respective names listed alphabetically under each column.

# Note: Print NULL when there are no more names corresponding to an occupation.
#####=================================================================================##################

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

spark=SparkSession.builder.appName('hackerrank19.com').master("local[*]").getOrCreate()
# Set Spark log level
spark.sparkContext.setLogLevel("ERROR")

data = [
    ("Samantha", "Doctor"),
    ("Julia", "Actor"),
    ("Maria", "Actor"),
    ("Meera", "Singer"),
    ("Ashley", "Professor"),
    ("Ketty", "Professor"),
    ("Christeen", "Professor"),
    ("Jane", "Actor"),
    ("Jenny", "Doctor"),
    ("Priya", "Singer")
]

columns = ["Name", "Occupation"]

occupations_df = spark.createDataFrame(data, columns)

print("_______________________________")
print("sample data:")
occupations_df.show()
print("_______________________________")

# Pivot the Occupation column in OCCUPATIONS so that each Name is sorted alphabetically 
# and displayed underneath its corresponding Occupation. The output should consist 
# of four columns (Doctor, Professor, Singer, and Actor) in that specific order,
# with their respective names listed alphabetically under each column.

# Note: Print NULL when there are no more names corresponding to an occupation.
###################>>>>>>>>>><<<<<<<<<<<< solution
windowsSpec=Window.partitionBy("Occupation").orderBy("Name")
ranked_df=occupations_df.withColumn("row_num", row_number().over(windowsSpec))

ranked_df.groupBy("row_num").pivot("Occupation").agg({"Name" : "first"}).orderBy("row_num").show()