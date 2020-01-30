# from pyspark.sql.functions import lower, col, udf
# import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql import Window
# unix_timestamp, from_unixtime,
from pyspark.sql import SparkSession, SQLContext
from math import cos, asin, sqrt, pi
from pyspark.sql.types import FloatType

if __name__ == '__main__':
    scSpark = SparkSession \
    .builder \
    .appName("reading csv") \
    .getOrCreate()


data_file ='/home/duryan/Documents/ws-data-spark/data/DataSample.csv'
poi_file ='/home/duryan/Documents/ws-data-spark/data/POIList.csv'

df = scSpark.read.csv(data_file, header=True,sep=",").cache()
df2 = scSpark.read.csv(poi_file, header=True,sep=",").cache()

# remove unnecessary characters and convert to lowercase
df = df.toDF(*[c.replace(" ", "").replace("_", "").lower() for c in df.columns])
df2 = df2.toDF(*[c.replace(" ", "").replace("_", "").lower() for c in df2.columns])

# convert to datetime format
df = df.withColumn("timest", to_timestamp(df["timest"])).withColumn("longitude", df["longitude"].cast("float")).withColumn("latitude", df["latitude"].cast("float"))

df2 = df2.withColumn("longitude", df2["longitude"].cast("float")).withColumn("latitude", df2["latitude"].cast("float")).withColumnRenamed("latitude", "poi_latitude").withColumnRenamed("longitude", "poi_longitude")

### 1. Cleanup
# drop duplicates
df = df.dropDuplicates(["timest", "latitude", "longitude"])

### 2. Label

# setup POI distance calculation
# Haversine equation (in kilometres, km)
def distance_calc(lat, lon, lat2, lon2):
    p = pi/180
    poi = 12742 * asin(sqrt(0.5 - cos((lat2 - lat) * p)/2 + cos(lat * p) * cos(lat2 * p) * (1 - cos((lon2 - lon) * p)) / 2))
    return poi

# convert to user defined function
poi_udf = udf(lambda lat, lon, lat2, lon2: distance_calc(lat, lon, lat2, lon2), FloatType())

# setup a Window function to reduce a crossJoined dataframe to only those with closest POI
w1 = Window.partitionBy(col("id"))

df = df.crossJoin(df2).withColumn("distance_calc", poi_udf(df.latitude, df.longitude, df2.poi_latitude, df2.poi_longitude)).withColumn("closest_dist", min(col("distance_calc")).over(w1)).where(col("closest_dist") == col("distance_calc")).drop(col('distance_calc')).filter(df.longitude<0)

# consideration for this because POI1 and POI2 are the exact same
### df.dropDuplicates(['id'])

### 3. Analysis
### 3.1 Calculate average and standard deviation of the distance between POI to each of its assigned requests
df_temp = df.groupBy(df.poiid).agg(avg(df.closest_dist).alias("mean"), stddev(df.closest_dist).alias("stddev"))

### 3.2 At each POI draw a circle (center at POI) that includes all its assigned requests. Calculate radius and density (requests/area) for each POI

# done in Pandas
