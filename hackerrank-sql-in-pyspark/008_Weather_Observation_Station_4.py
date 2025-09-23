#Find the difference between the total number of CITY entries in the table 
# and the number of distinct CITY entries in the table.


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct


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

#Find the difference between the total number of CITY entries in the table 
# and the number of distinct CITY entries in the table.

###################>>>>>>>>>><<<<<<<<<<<< solution 1
total_city_count=station_df.select(station_df["CITY"]).count()
distinct_city_count=station_df.select(station_df["CITY"]).distinct().count()

diff_city_count=total_city_count-distinct_city_count
print(diff_city_count)



###################>>>>>>>>>><<<<<<<<<<<< solution 2

station_df.agg(
    (count("CITY")- countDistinct("CITY")).alias("diff_city_count")
).show()


