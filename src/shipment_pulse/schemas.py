from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PORT_SCHEMA = StructType(
    [
        StructField("port_code", StringType(), True),
        StructField("port_name", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("region", StringType(), True),
    ]
)

SHIPMENTS_CDC_SCHEMA = StructType(
    [
        StructField("booking_reference", StringType(), True),
        StructField("cargo_weight_kg", DoubleType(), True),
        StructField("container_count", LongType(), True),
        StructField("customer_tier", StringType(), True),
        StructField("destination_port", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("operation_timestamp", TimestampType(), True),
        StructField("origin_port", StringType(), True),
        StructField("planned_arrival_time", TimestampType(), True),
        StructField("planned_departure_time", TimestampType(), True),
        StructField("shipment_id", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

SHIPMENT_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("location_code", StringType(), True),
        StructField("payload_version", LongType(), True),
        StructField("received_at", TimestampType(), True),
        StructField("shipment_id", StringType(), True),
        StructField("source_system", StringType(), True),
    ]
)