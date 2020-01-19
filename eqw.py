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
df = df.withColumn("timest", to_timestamp(df["timest"])).withColumn("longitude", df["longitude"].cast("float")).withColumn("latitude", df["latitude"].cast("float"))



df2 = df2.withColumn("longitude", df2["longitude"].cast("float")).withColumn("latitude", df2["latitude"].cast("float"))


# drop duplicates
df = df.dropDuplicates(["timest", "latitude", "longitude"])

# setup inner join
t1 = df.alias("t1")
t2 = df2.alias("t2")

t_join = t1.join(t2, (t1.id == t2.poiid) | (t1.id == t2.latitude) | (t1.id == t2.longitude), how='left')\.select("t1.*", )

d = df.union(df2)

>>> lol3  = lol2.groupBy(df.id).min("sum").select(f.col("min(sum)").alias("totlas"), df.id).show()


# closest POI
p = pi/180

# poii = min([12742 * asin(sqrt(0.5 - cos((df2.collect()[i]["latitude"] - col("latitude")) * p)/2 + cos(col("latitude") * p) * cos(df2.collect()[i]["latitude"] * p) * (1 - cos((df2.collect()[i]["longitude"] - col("longitude")) * p)) / 2)) for i in range(df2.count())])

# 12742 * asin(sqrt(a))

df = df.withColumn("poi_dist", min([12742 * asin(sqrt(0.5 - cos((df2.collect()[i]["latitude"] - df.collect()[x]["latitude"]) * p)/2 + cos(df.collect()[x]["latitude"] * p) * cos(df2.collect()[i]["latitude"] * p) * (1 - cos((df2.collect()[i]["longitude"] - df.collect()[x]["longitude"]) * p)) / 2)) for i in range(df2.count())] for x in range(df.count())))

def poi_calc(latitude, longitude, lat2, lon2):
    p = pi/180
    return min([12742 * asin(sqrt(0.5 - cos((lat2 - latitude) * p)/2 + cos(latitude * p) * cos(lat2 * p) * (1 - cos((lon2 - longitude) * p)) / 2)) for i in range(df2.count())])

poi_calc_udf = udf(lambda latitude, longitude, lat2, lon2: poi_calc(latitude, longitude, lat2, lon2), FloatType())

df = df.withColumn("poi_dist", poi_calc_udf(df.latitude, df.longitude, df2))







df.select("latitude", "longitude", df2.select("poi","latitude", "longitude").alias("xdd")).show()


[12742 * asin(sqrt(0.5 - cos((df2.collect()[i]["latitude"] - df.collect()[0]["latitude"]) * p)/2 + cos(df.collect()[0]["latitude"] * p) * cos(df2.collect()[i]["latitude"] * p) * (1 - cos((df2.collect()[i]["longitude"] - df.collect()[0]["longitude"]) * p)) / 2)) for i in range(df2.count())]
