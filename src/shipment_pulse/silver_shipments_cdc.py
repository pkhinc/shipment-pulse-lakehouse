from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_SHIP_CDC_PATH = "/opt/project/data/generated/bronze/shipments_cdc"
SILVER_SHIP_CDC_PATH = "/opt/project/data/generated/silver/shipments_cdc"


def main() -> None:
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("shipment-pulse-silver-validation")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        shipments_cdc_df = spark.read.parquet(BRONZE_SHIP_CDC_PATH)
        shipments_cdc_df = shipments_cdc_df.filter(
            F.col("shipment_id").isNotNull() & F.col("operation_timestamp").isNotNull() & F.col("operation").isNotNull()
        )
        shipments_cdc_df = shipments_cdc_df.filter(F.col("cargo_weight_kg") >= 0)
        shipments_cdc_df = shipments_cdc_df.filter(F.col("container_count") >= 0)

        # sprawdzenie czy origin_port i destination_port istnieją w tabeli ports
        ports_df = spark.read.parquet("/opt/project/data/generated/silver/ports")

        invalid_dest_df = shipments_cdc_df.join(
            ports_df, 
            shipments_cdc_df["destination_port"] == ports_df["port_code"], 
            "left_anti")

        invalid_org_df = shipments_cdc_df.join(
            ports_df, 
            shipments_cdc_df["origin_port"] == ports_df["port_code"], 
            "left_anti"
        )

        if invalid_dest_df.count() > 0:
            raise ValueError("Invalid destination ports found in the dataset.")

        if invalid_org_df.count() > 0:
            raise ValueError("Invalid origin ports found in the dataset.")

        # sprawdzenie dat - czy arrival nie jest wczesniej niz departure
        invalid_date_df = shipments_cdc_df.filter((F.col("planned_arrival_time") < F.col("planned_departure_time")) 
                                                | (F.col("planned_arrival_time").isNull()) | (F.col("planned_departure_time").isNull()))
        

        if invalid_date_df.count() > 0:
            raise ValueError("Invalid dates found in the dataset: arrival_time is earlier than departure_time or one of the times is null.")

    
        shipments_cdc_df.write.mode("overwrite").parquet(SILVER_SHIP_CDC_PATH)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()    