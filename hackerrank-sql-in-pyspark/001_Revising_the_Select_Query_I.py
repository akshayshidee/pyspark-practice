# Query all columns for all American cities with populations larger than 100000, CountryCode = USA
import pyspark
from pyspark.sql import SparkSession


spark=SparkSession.builder.appName('hackerrank01.com').master("local[*]").getOrCreate()
# Set Spark log level
spark.sparkContext.setLogLevel("ERROR")

# Sample data
city_data = [
    (1, "New York", "USA", "New York", 8175133),
    (2, "Los Angeles", "USA", "California", 3792621),
    (3, "Chicago", "USA", "Illinois", 2695598),
    (4, "Houston", "USA", "Texas", 2129784),
    (5, "Phoenix", "USA", "Arizona", 1445632),
    (6, "San Antonio", "USA", "Texas", 1327407),
    (7, "San Diego", "USA", "California", 1307402),
    (8, "Dallas", "USA", "Texas", 1197816),
    (9, "San Jose", "USA", "California", 945942),
    (10, "Austin", "USA", "Texas", 790390),
    (11, "Jacksonville", "USA", "Florida", 821784),
    (12, "SmallTown", "USA", "Ohio", 50000),  
    (13, "Toronto", "CAN", "Ontario", 2615060), 
    (14, "London", "GBR", "England", 8673713)   
]

# Schema
columns = ["ID", "Name", "CountryCode", "District", "Population"]

# Create DataFrame
city_df = spark.createDataFrame(city_data, columns)

print("_____________________________")
print("Sample CITY table:")
city_df.show()

###solution

city_df.filter((city_df["Population"]>100000) & (city_df["CountryCode"]=="USA") ).show()