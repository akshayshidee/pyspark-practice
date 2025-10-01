# Write a query that prints a list of employee names (i.e.: the name attribute)
# from the Employee table in alphabetical order.
# where employee_id is an employee's ID number, name is their name, 
# months is the total number of months they've been working for the company, and salary is their monthly salary.
#####=================================================================================##################

import pyspark
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('hackerrank18.com').master("local[*]").getOrCreate()
# Set Spark log level
spark.sparkContext.setLogLevel("ERROR")

data = [
    (1, "Julia", 24, 3000),
    (2, "Ashley", 36, 3500),
    (3, "Belvet", 12, 2500),
    (4, "John", 48, 4000),
    (5, "Sam", 18, 2800),
    (6, "Rob", 30, 3200),
]

# Define schema columns
columns = ["employee_id", "name", "months", "salary"]

# Create DataFrame
employee_df = spark.createDataFrame(data, columns)

print("_______________________________")
print("sample data:")
employee_df.show()
print("_______________________________")

# Write a query that prints a list of employee names (i.e.: the name attribute)
# from the Employee table in alphabetical order.
###################>>>>>>>>>><<<<<<<<<<<< solution
employee_df.select(employee_df["name"]).sort(employee_df["name"].asc()).show()
