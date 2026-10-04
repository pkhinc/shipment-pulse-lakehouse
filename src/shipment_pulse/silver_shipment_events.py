from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_SHIP_EVENTS_PATH = "/opt/project/data/generated/bronze/shipment_events"
SILVER_SHIP_EVENTS_PATH = "/opt/project/data/generated/silver/shipment_events"

def main() -> None:
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("shipment-pulse-silver-validation")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        shipment_events_df = spark.read.parquet(BRONZE_SHIP_EVENTS_PATH)
        shipment_events_df = shipment_events_df.filter(
            F.col("shipment_id").isNotNull() & F.col("event_id").isNotNull()
            & F.col("event_time").isNotNull() & F.col("received_at").isNotNull()
        )

        # sprawdzenie czy porty istnieją w tabeli ports
        ports_df = spark.read.parquet("/opt/project/data/generated/silver/ports")
                 
        invalid_port_df = shipment_events_df.join(
            ports_df, 
            shipment_events_df["location_code"] == ports_df["port_code"], 
            "left_anti")

        if invalid_port_df.count() > 0:
            raise ValueError("Invalid location ports found in the dataset.")

        # sprawdzenie czy received at nie jest wczesniej niz event_time
        invalid_time_df = shipment_events_df.filter(F.col("received_at") < F.col("event_time"))

        if invalid_time_df.count() > 0:
            raise ValueError("Invalid time values found in the dataset.")

        #sprawdzenie czy event_id nie jest zduplikowany
        duplicate_event_df = shipment_events_df.groupBy("event_id").agg(F.count("*").alias("event_count")).filter(F.col("event_count") > 1)

        if duplicate_event_df.count() > 0:
            raise ValueError("Duplicate event IDs found in the dataset.")

        shipment_events_df.write.mode("overwrite").parquet(SILVER_SHIP_EVENTS_PATH)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()