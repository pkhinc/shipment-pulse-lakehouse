from pyspark import pipelines as dp
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.active()

SILVER_PATH = "/opt/project/data/generated/silver"

@dp.materialized_view
def shipment_summary():
    """
    Materialized view for shipment summary.
    """
    shipments_df = spark.read.parquet(f"{SILVER_PATH}/shipments_cdc")
    ports_df = spark.read.parquet(f"{SILVER_PATH}/ports")
    events_df = spark.read.parquet(f"{SILVER_PATH}/shipment_events")

    origin_ports_df = ports_df.select(
        F.col("port_code").alias("origin_port"),
        F.col("port_name").alias("origin_port_name")
    )

    shipments_df = shipments_df.join(
        origin_ports_df,
        on="origin_port",
        how="left"
    )

    destination_ports_df = ports_df.select(
        F.col("port_code").alias("destination_port"),   
        F.col("port_name").alias("destination_port_name")
    )

    shipments_df = shipments_df.join(
        destination_ports_df,
        on="destination_port",
        how="left"
    )

    events_df = events_df.withColumn(
            "event_latency_minutes",
            F.timestamp_diff("SECOND", "event_time", "received_at") / 60
    )

    event_summary_df = events_df.groupBy("shipment_id").agg(
        F.count("*").alias("event_count"),
        F.avg("event_latency_minutes").alias("avg_event_latency_minutes"),
    )

    shipments_df = shipments_df.join(
        event_summary_df,
        on="shipment_id",
        how="left"
    )

    shipments_df = shipments_df.withColumn(
        "planned_transit_hours",
        F.timestamp_diff("SECOND", "planned_departure_time", "planned_arrival_time") / 3600
    )

    weight_window = Window.partitionBy("origin_port").orderBy(F.col("cargo_weight_kg").desc())

    shipments_df = shipments_df.withColumn("weight_rank_in_origin", F.dense_rank().over(weight_window))

    return shipments_df


@dp.materialized_view
def route_summary():
    shipments_df = spark.read.table("shipment_summary")

    route_summary_df = shipments_df.groupBy("origin_port", "destination_port").agg(
        F.count("*").alias("shipment_count"),
    )

    return route_summary_df