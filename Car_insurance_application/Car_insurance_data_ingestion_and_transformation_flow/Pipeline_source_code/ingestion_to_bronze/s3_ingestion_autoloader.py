import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

level = "bronze"

############### meta_data_images #################3
@dlt.table(
    name = f"claim_images_meta_{level}",
    comment="Raw accident claim images metadata from the source",
    table_properties={"quality":level}
)

def raw_images():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .load("/Volumes/sample_claims_dev/00_landing/claims/claims/metadata/")
    )

############# ingest_training_images ################
@dlt.table(
    name = f"training_images_{level}",
    comment="Raw accident training images for machine learning",
    table_properties={"quality":level}
)

def raw__training_images():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","BINARYFILE")
        .load("/Volumes/sample_claims_dev/00_landing/claims/training_imgs/")
    )

####################### autoloader_data ###################
landing_catalog = "sample_claims_dev"
landing_schema = "00_landing"

base_path = f"/Volumes/{landing_catalog}/{landing_schema}/claims/claims"
source_path = f"{base_path}/images"
archive_path = f"{base_path}/archive"

@dlt.table(
    name = f"claim_images_{level}",
    comment="Raw accident claim images with archiving enabled"
)
def claim_images():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","BINARYFILE")
        .option("cloudFiles.cleanSource","MOVE")
        .option("cloudFiles.cleanSource.retentionDuration","1 minute")
        .option("cloudFiles.cleanSource.moveDestination",archive_path)
        .load(source_path)
    )
