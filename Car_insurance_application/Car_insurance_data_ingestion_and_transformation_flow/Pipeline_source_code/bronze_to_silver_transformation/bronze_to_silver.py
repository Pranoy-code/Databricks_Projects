import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

catalog = "sample_claims_dev"
bronze_schema = "01_bronze"
silver_schema = "02_silver"
level_old = "bronze"
level = "silver"

##### cleaning Telematics ######
@dlt.table(
    name = f"{catalog}.{silver_schema}.telematics_{level}",
    comment="Cleaned telematics data",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_coordinates", "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180")

def telematics():
    return (
        dlt.readStream(f"{catalog}.{bronze_schema}.telematics_data_{level_old}").withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")).drop("_rescued_data")
    )

###### Clean Policy #######
@dlt.table(
    name = f"{catalog}.{silver_schema}.policy_{level}",
    comment="Cleaned policy data",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_policy_number", "POLICY_NO IS NOT NULL")

def policy():
    return (
        dlt.readStream(f"{catalog}.{bronze_schema}.policy_{level_old}").withColumn("premium",abs("premium")).drop("_rescued_data")
)

###### Clean claim ##############
@dlt.table(
    name = f"{catalog}.{silver_schema}.claim_{level}",
    comment="Cleaned claim data",
    table_properties={"quality": "silver"}
)
@dlt.expect_all({
    "valid_claim_number": "claim_no IS NOT NULL",
    "valid_incident_hour": "hour BETWEEN 0 AND 23"
})
def claim():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.claims_{level_old}")
    return(
        df.withColumn("claim_date", to_date(col("claim_date")))
        .withColumn("incident_date", to_date(col("date"), "yyyy-MM-dd")).withColumn("license_issue_date", to_date(col("license_issue_date"), "dd-MM-yyyy")).drop("_rescued_data")
    )

###### Clean Customer #######
@dlt.table(
    name = f"{catalog}.{silver_schema}.customer_{level}",
    comment="Cleaned customer data",
    table_properties={"quality": "silver"}
)
@dlt.expect_all({"valid_customer_id": "customer_id IS NOT NULL"})

def customer():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.customer_{level_old}")

    name_normalized = when(
        size(split(trim(col("name")), ",")) == 2,
        concat(
            initcap(trim(split(col("name"), ",").getItem(1))), lit(" "),
            initcap(trim(split(col("name"), ",").getItem(0)))
        )
    ).otherwise(initcap(trim(col("name"))))

    return (
        df
        .withColumn("date_of_birth", to_date(col("date_of_birth"), "dd-MM-yyyy"))
        .withColumn("firstname", split(name_normalized, " ").getItem(0))
        .withColumn("lastname", split(name_normalized, " ").getItem(1))
        .withColumn("address", concat(col("BOROUGH"), lit(", "), col("ZIP_CODE")))
        .drop("name", "_rescued_data")
    )

################# Clean Training Images ##################
@dlt.table(
    name = f"{catalog}.{silver_schema}.training_images_{level}",
    comment="Cleaned training images data",
    table_properties={"quality": "silver"}
)
def training_images():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.training_images_{level_old}")

    return(
        df.withColumn("label",regexp_extract("path", r"/(\d+)-([a-zA-Z]+)(?: \(\d+\))?\.png$", 2))
    )

############ clean claim Images ###############
@dlt.table(
    name = f"{catalog}.{silver_schema}.claim_images_{level}",
    comment="Cleaned claim images data",
    table_properties={"quality": "silver"}
)

def claim_images():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.claim_images_{level_old}")
    return (df.withColumn("image_name",regexp_extract(col("path"), r".*/(.*?.jpg)", 1)))
