# Generate the following two result sets:

# Query an alphabetically ordered list of all names in OCCUPATIONS, 
# immediately followed by the first letter of each profession a
# s a parenthetical (i.e.: enclosed in parentheses). 
# For example: AnActorName(A), ADoctorName(D), AProfessorName(P), and ASingerName(S).
# Query the number of ocurrences of each occupation in OCCUPATIONS. 
# Sort the occurrences in ascending order, and output them in the following format:

# There are a total of [occupation_count] [occupation]s.
# where [occupation_count] is the number of occurrences of an occupation in 
# OCCUPATIONS and [occupation] is the lowercase occupation name. 
# If more than one Occupation has the same [occupation_count], they should be ordered alphabetically.
# Note: There will be at least two entries in the table for each type of occupation.
#####=================================================================================##################

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat,lit,substring,count,lower,col

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

# Generate the following two result sets:
# Query an alphabetically ordered list of all names in OCCUPATIONS, 
# immediately followed by the first letter of each profession as
#  a parenthetical (i.e.: enclosed in parentheses). 
# For example: AnActorName(A), ADoctorName(D), AProfessorName(P), and ASingerName(S).
###################>>>>>>>>>><<<<<<<<<<<< solution
occupations_df.select(
    concat(
        occupations_df["Name"]
        ,lit("(")
        , substring(occupations_df["Occupation"],1,1)
        , lit(")")
        )
    .alias("formated"))\
    .sort(occupations_df["Name"].asc())\
        .show()



# Query the number of ocurrences of each occupation in OCCUPATIONS. 
# Sort the occurrences in ascending order, and output them in the following format:

# There are a total of [occupation_count] [occupation]s.
# where [occupation_count] is the number of occurrences of an occupation in 
# OCCUPATIONS and [occupation] is the lowercase occupation name. 
# If more than one Occupation has the same [occupation_count], they should be ordered alphabetically.
# Note: There will be at least two entries in the table for each type of occupation.
###################>>>>>>>>>><<<<<<<<<<<< solution
occupations_df.groupBy(occupations_df["Occupation"])\
    .agg(count("*").alias("occupation_count"))\
        .sort("occupation_count","Occupation")\
            .select(
                concat(
                    lit("There are a total of "),
                    col("occupation_count").cast("string"),
                    lit(" "),
                    lower("Occupation"),
                    lit("s")
                    ).alias("result")
            )\
        .show(truncate=False)