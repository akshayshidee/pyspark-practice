#Query a list of CITY names from STATION for cities that have an even ID number. 
# Print the results in any order, but exclude duplicates from the answer.


import pyspark
from pyspark.sql import SparkSession


spark=SparkSession.builder.appName('hackerrank7.com').master("local[*]").getOrCreate()
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

###solution

station_df.filter(station_df["ID"]%2==0).select(station_df["CITY"]).distinct().show()