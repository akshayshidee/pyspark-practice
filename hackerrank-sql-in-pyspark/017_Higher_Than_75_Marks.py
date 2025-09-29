# Query the Name of any student in STUDENTS who scored higher than  Marks.
# Order your output by the last three characters of each name. 
# If two or more students both have names ending in the same last 
# three characters (i.e.: Bobby, Robby, etc.), secondary sort them by ascending ID.

# Only Ashley, Julia, and Belvet have Marks > . If you look at the last three characters 
# of each of their names, there are no duplicates and 'ley' < 'lia' < 'vet'.

#####=================================================================================##################

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring,length,expr


spark=SparkSession.builder.appName('hackerrank17.com').master("local[*]").getOrCreate()
# Set Spark log level
spark.sparkContext.setLogLevel("ERROR")

data = [
    (1, "Ashley", 81),
    (2, "Julia", 88),
    (3, "Belvet", 90),
    (4, "Bobby", 76),
    (5, "Robby", 82),
    (6, "Chris", 65),
    (7, "Daisy", 74),
    (8, "Tommy", 80)
]

columns = ["ID", "Name", "Marks"]

students_df = spark.createDataFrame(data, columns)
print("_____________________________")
print("Sample data:")
students_df.show()

# Query the Name of any student in STUDENTS who scored higher than  Marks.
# Order your output by the last three characters of each name. 
# If two or more students both have names ending in the same last 
# three characters (i.e.: Bobby, Robby, etc.), secondary sort them by ascending ID.
###################>>>>>>>>>><<<<<<<<<<<< solution

students_df.filter(students_df["Marks"]>75).sort\
    (expr("substring(Name,length(Name)-2,3)").asc(),students_df["ID"].asc())\
        .select(students_df["Name"]).show()

        