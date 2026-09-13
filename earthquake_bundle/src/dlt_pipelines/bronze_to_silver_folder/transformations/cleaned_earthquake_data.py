from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    count,
    count_if,
    from_json,
    explode,
    from_unixtime,
    current_timestamp
)
import dlt
from pyspark.sql.types import *

volume_path = "/Volumes/earthquake_dev_catalog/bronze/earthquake_volume"

properties_schema = StructType(
    [
        StructField("mag", StringType()),
        StructField("place", StringType()),
        StructField("time", StringType()),
        StructField("status", StringType()),
        StructField("tsunami", StringType()),
        StructField("type", StringType()),
        StructField("url", StringType()),
        StructField("detail", StringType()),
        StructField("felt", StringType()),
        StructField("cdi", StringType()),
        StructField("mmi", StringType()),
        StructField("alert", StringType()),
        StructField("sig", StringType()),
        StructField("net", StringType()),
        StructField("code", StringType()),
        StructField("ids", StringType()),
        StructField("sources", StringType()),
        StructField("types", StringType()),
        StructField("nst", StringType()),
        StructField("dmin", StringType()),
        StructField("rms", StringType()),
        StructField("gap", StringType()),
        StructField("magType", StringType()),
        StructField("title", StringType()),
    ]
)

geometry_schema = StructType([StructField("coordinates", ArrayType(DoubleType()))])

feature_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("properties", properties_schema),
        StructField("geometry", geometry_schema),
    ]
)

schema = ArrayType(feature_schema)


@dlt.table(name="earthquake_data_tbl")
def earthquake_data():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
        .withColumn("_load_timestamp", current_timestamp())
    )

    df = df.withColumn("parsed_data", from_json(col("features"), schema))

    df = df.select(explode(col("parsed_data")).alias("features"))
    df = df.select(
        col("features.properties.mag").alias("mag"),
        col("features.properties.place").alias("place"),
        col("features.properties.time").alias("time"),
        col("features.properties.status").alias("status"),
        col("features.properties.tsunami").alias("tsunami"),
        col("features.properties.type").alias("type"),
        col("features.properties.url").alias("url"),
        col("features.properties.detail").alias("detail"),
        col("features.properties.felt").alias("felt"),
        col("features.properties.cdi").alias("cdi"),
        col("features.properties.mmi").alias("mmi"),
        col("features.properties.alert").alias("alert"),
        col("features.properties.sig").alias("sig"),
        col("features.properties.net").alias("net"),
        col("features.properties.code").alias("code"),
        col("features.properties.ids").alias("ids"),
        col("features.properties.sources").alias("sources"),
        col("features.properties.types").alias("types"),
        col("features.properties.nst").alias("nst"),
        col("features.properties.dmin").alias("dmin"),
        col("features.properties.rms").alias("rms"),
        col("features.properties.gap").alias("gap"),
        col("features.properties.magType").alias("magType"),
        col("features.properties.title").alias("title"),
        col("features.id").alias("id"),
        col("features.geometry.coordinates")[0].alias("longitude"),
        col("features.geometry.coordinates")[1].alias("latitude"),
        col("features.geometry.coordinates")[2].alias("depth"),
    )
    df = (
        df.withColumn("time", from_unixtime(col("time") / 1000).cast("timestamp"))
        .withColumn("mag", col("mag").cast("double"))
        .withColumn("mmi", col("mmi").cast("double"))
        .withColumn("sig", col("sig").cast("double"))
        .withColumn("cdi", col("cdi").cast("double"))
        .withColumn("nst", col("nst").cast("double"))
        .withColumn("dmin", col("dmin").cast("double"))
        .withColumn("gap", col("gap").cast("double"))
        .withColumn("tsunami", col("tsunami").cast("double"))
        .withColumn("felt", col("felt").cast("double"))
        )
    return df
