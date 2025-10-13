# Write a query that prints a list of employee names (i.e.: the name attribute) 
# for employees in Employee having a salary greater than 2000 per month who have been employees
# for less than 10  months. Sort your result by ascending employee_id..
#####=================================================================================##################

import pyspark
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('hackerrank19.com').master("local[*]").getOrCreate()
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
# for employees in Employee having a salary greater than 2000 per month who have been employees
# for less than 10  months. Sort your result by ascending employee_id..
###################>>>>>>>>>><<<<<<<<<<<< solution
employee_df.filter((employee_df["salary"]>2000) & (employee_df["months"]<20))\
        .sort(employee_df["employee_id"].asc())\
                .select(employee_df["name"])\
        .show()
        