from pyspark.sql.functions import lower, col, udf
import pyspark.sql.functions as f
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
df = df.withColumn("timest", f.to_timestamp(df["timest"])).withColumn("longitude", df["longitude"].cast("float")).withColumn("latitude", df["latitude"].cast("float"))

df2 = df2.withColumn("longitude", df2["longitude"].cast("float")).withColumn("latitude", df2["latitude"].cast("float"))

# drop duplicates
df = df.dropDuplicates(["timest", "latitude", "longitude"])

# setup POI distance calculation
def poi_calc(lat, lon, lat2, lon2):
    p = pi/180
    poi = 12742 * asin(sqrt(0.5 - cos((lat2 - lat) * p)/2 + cos(lat * p) * cos(lat2 * p) * (1 - cos((lon2 - lon) * p)) / 2))
    return poi

# convert to user defined function
poi_udf = f.udf(lambda lat, lon, lat2, lon2: poi_calc(lat, lon, lat2, lon2), FloatType())

# setup a Window function to reduce a crossJoined dataframe to only those with closest POI
ww = Window.partitionBy(col("id"))

df = df.crossJoin(df2).withColumn("poi_calc", poi_udf(df.latitude, df.longitude, df2.latitude, df2.longitude)).withColumn("min_poi", f.min(col("poi_calc")).over(ww)).where(col("min_poi") == col("poi_calc")).drop(col('poi_calc'))

# consideration for this because POI1 and POI2 are the exact same
### df.dropDuplicates(['id'])
