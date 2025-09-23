# Query the two cities in STATION with the shortest and longest CITY names, 
# as well as their respective lengths (i.e.: number of characters in the name). 
# If there is more than one smallest or largest city,
# choose the one that comes first when ordered alphabetically.

# where LAT_N is the northern latitude and LONG_W is the western longitude.

# Sample Input
# For example, CITY has four entries: DEF, ABC, PQRS and WXY.

# Sample Output
# ABC 3
# PQRS 4

# Explanation
# When ordered alphabetically, the CITY names are listed as ABC, DEF, PQRS, and WXY, with lengths  and . 
# The longest name is PQRS, but there are  options for shortest named city. 
# Choose ABC, because it comes first alphabetically.

# Note
# You can write two separate queries to get the desired output. It need not be a single query.

#####=================================================================================##################

import pyspark
from pyspark.sql import SparkSession


spark=SparkSession.builder.appName('hackerrank8.com').master("local[*]").getOrCreate()
# Set Spark log level
spark.sparkContext.setLogLevel("ERROR")

station_data = [
    (1, "New York", "NY", 40.7128, -74.0060),
    (2, "Los Angeles", "CA", 34.0522, -118.2437),
    (3, "Chicago", "IL", 41.8781, -87.6298),
    (4, "Houston", "TX", 29.7604, -95.3698),
    (5, "Phoenix", "AZ", 33.4484, -112.0740),
    (6, "Seattle", "WA", 47.6062, -122.3321),
    (7, "Boston", "MA", 42.3601, -71.0589),
    (8, "Miami", "FL", 25.7617, -80.1918),
    (9, "Los Angeles", "CA", 34.0522, -118.2437),
]

columns = ["ID", "CITY", "STATE", "LAT_N", "LONG_W"]

station_df = spark.createDataFrame(data=station_data, schema=columns)

print("_____________________________")
print("Sample STATION table:")
station_df.show()

# Query the two cities in STATION with the shortest and longest CITY names, 
# as well as their respective lengths (i.e.: number of characters in the name). 
# If there is more than one smallest or largest city,
# choose the one that comes first when ordered alphabetically.

###################>>>>>>>>>><<<<<<<<<<<< solution





