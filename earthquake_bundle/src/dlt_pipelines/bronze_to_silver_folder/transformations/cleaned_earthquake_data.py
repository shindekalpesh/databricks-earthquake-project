from pyspark import pipelines as dp
from pyspark.sql.functions import col
import dlt

volume_path = "/Volumes/earthquake_dev_catalog/bronze/earthquake_volume"

@dlt.table(name='earthquake_data_tbl')
def earthquake_data():
    return spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format', 'json')\
            .load(volume_path)