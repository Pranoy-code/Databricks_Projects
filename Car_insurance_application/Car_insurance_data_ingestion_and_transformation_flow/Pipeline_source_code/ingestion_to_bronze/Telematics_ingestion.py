import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

level = "bronze"
@dlt.table(
    name=f"Telematics_data_{level}",
    comment = "Telematics streaming data obtained from the IoT devices",
    table_properties = {"quality":"bronze"}
)
def Telematics_data():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","parquet")
        .load("/Volumes/sample_claims_dev/00_landing/telematics_raw/telematics/")
    )