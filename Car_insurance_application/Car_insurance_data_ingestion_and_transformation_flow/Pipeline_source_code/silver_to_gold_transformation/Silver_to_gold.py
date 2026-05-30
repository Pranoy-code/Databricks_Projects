import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from typing import Iterator
import random


catalog = "sample_claims_dev"
silver_schema = "02_silver"
gold_schema = "03_gold"
level_old = "silver"
level = "gold"

def geocode(address):
    try:
        return pd.Series({'latitude':random.uniform(-90,90),'longitude':random.uniform(-180,180)})
    except Exception as e:
        print(f"error getting lat/long : {e}")
    return pd.Series({'latitude':None,'longitude':None})

@pandas_udf("latitude float, longitude float")
def get_lat_long(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    for address in batch_iter:
        yield address.apply(lambda x: geocode(x))

@dlt.table(
    name = f"{catalog}.{gold_schema}.aggregated_telematics_{level}",
    comment = "Average telematics",
    table_properties = {"quality":"gold"}
)
def telematics(): 
    return (
        dlt.read(f"{catalog}.{silver_schema}.telematics_{level_old}")
        .groupBy("chassis_no")
        .agg(
            avg("speed").alias("telematics_speed"),
            avg("latitude").alias("telematics_latitude"),
            avg("longitude").alias("telematics_longitude"),
        )
    )

############## customer-claim-policy ########################
@dlt.table(
    name = f"{catalog}.{gold_schema}.customer_claim_policy_{level}",
    comment = "customer_claim_policy",
    table_properties = {"quality":"gold"}
)
def customer_claim_policy(): 
    policy = dlt.readStream(f"{catalog}.{silver_schema}.policy_{level_old}")
    claim = dlt.readStream(f"{catalog}.{silver_schema}.claim_{level_old}")
    customer = dlt.readStream(f"{catalog}.{silver_schema}.customer_{level_old}")
    claim_policy = claim.join(policy, claim.policy_no == policy.POLICY_NO).drop(policy.POLICY_NO)
    return claim_policy.join(customer, claim_policy.CUST_ID == customer.customer_id)

############## customer-claim-policy-telematics ########################
@dlt.table(
    name = f"{catalog}.{gold_schema}.customer_claim_policy_telematics_{level}",
    comment = "customer_claim_policy_telematics",
    table_properties = {"quality":"gold"}
)
def customer_claim_policy_telematics():
    telematics = dlt.read(f"{catalog}.{gold_schema}.aggregated_telematics_{level}")
    claim_policy = dlt.readStream(f"{catalog}.{gold_schema}.customer_claim_policy_{level}")

    return (claim_policy.withColumn("lat_long",get_lat_long(col("address"))).join(telematics,on="chassis_no"))

