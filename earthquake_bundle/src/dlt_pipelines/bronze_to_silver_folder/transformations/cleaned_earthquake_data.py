from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, count_if, from_json, explode
import dlt
from pyspark.sql.types import *

volume_path = "/Volumes/earthquake_dev_catalog/bronze/earthquake_volume"

properties_schema = StructType(
    [
        StructField('mag', StringType()),
        StructField('place', StringType()),
        StructField('time', StringType()),
        StructField('status', StringType()),
        StructField('tsunami', StringType()),
        StructField('type', StringType()),
        StructField('url', StringType()),
        StructField('detail', StringType()),
        StructField('felt', StringType()),
        StructField('cdi', StringType()),
        StructField('mmi', StringType()),
        StructField('alert', StringType()),
        StructField('sig', StringType()),
        StructField('net', StringType()),
        StructField('code', StringType()),
        StructField('ids', StringType()),
        StructField('sources', StringType()),
        StructField('types', StringType()),
        StructField('nst', StringType()),
        StructField('dmin', StringType()),
        StructField('rms', StringType()),
        StructField('gap', StringType()),
        StructField('magType', StringType()),
        StructField('title', StringType()),
    ]
)

geometry_schema = StructType(
    [
        StructField('coordinates', ArrayType(DoubleType()))
    ]
)

feature_schema = StructType(
    [
        StructField('id', StringType()),
        StructField('properties', properties_schema),
        StructField('geometry', geometry_schema)
    ]
)

schema = ArrayType(feature_schema)

@dlt.table(name='earthquake_data_tbl')
def earthquake_data():
    df = spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format', 'json')\
            .load(volume_path)

    df = df.withColumn('parsed_data',from_json(col('features'),schema))

    df = df.select(explode(col("parsed_data")).alias('features'))
    df = df.select(
        "features.properties.*",
        col("features.id").alias("id"),
        col("features.geometry.coordinates")[0].alias("longitude"),
        col("features.geometry.coordinates")[1].alias("latitude"),
        col("features.geometry.coordinates")[2].alias("depth"),
        
    )  
    return df

