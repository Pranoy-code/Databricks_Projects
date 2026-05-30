import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

level = "bronze"

##################Policy data ingestion##########################
@dlt.table(
    name = f"Policy_{level}",
    comment = "Policy data given from the company",
    table_properties={"quality":"bronze"}
)

def policy():
    return (spark.readStream.format("cloudFiles").
            option("cloudFiles.format", "csv").
            load("/Volumes/sample_claims_dev/00_landing/database_claims/policies_data/"))

##################customer data ingestion##########################
@dlt.table(
    name = f"Customer_{level}",
    comment = "customer data given by the company",
    table_properties={"quality":"bronze"}
)

def customer():
    return (spark.readStream.format("cloudFiles").
            option("cloudFiles.format", "csv").
            load("/Volumes/sample_claims_dev/00_landing/database_claims/customer_data/"))

#####################Claims_data_ingestion###############################
@dlt.table(
    name = f"Claims_{level}",
    comment = "claims data given from the company",
    table_properties={"quality":"bronze"}
)

def claims():
    return (spark.readStream.format("cloudFiles").
            option("cloudFiles.format", "csv").
            load("/Volumes/sample_claims_dev/00_landing/database_claims/claims_data/"))