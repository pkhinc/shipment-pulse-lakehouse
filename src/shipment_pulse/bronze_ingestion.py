from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from shipment_pulse.schemas import (
    PORT_SCHEMA,
    SHIPMENT_EVENTS_SCHEMA,
    SHIPMENTS_CDC_SCHEMA,
)

LANDING_PATH = "/opt/project/data/generated/landing"
BRONZE_PATH = "/opt/project/data/generated/bronze"
BATCH_ID = "batch_001"

def add_ingestion_metadata(dataframe: DataFrame, dataset_name: str,) -> DataFrame:
    return (
        dataframe.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_date", F.to_date("_ingested_at"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_source_dataset", F.lit(dataset_name))
        .withColumn("_batch_id", F.lit(BATCH_ID))
    )

def write_bronze_dataset(spark: SparkSession, dataset_name: str, dataframe: DataFrame,
) -> None:
    output_path = f"{BRONZE_PATH}/{dataset_name}"

    dataframe.write.mode("overwrite").parquet(output_path)

    saved_dataframe = spark.read.parquet(output_path)

    print(
        f"BRONZE_{dataset_name.upper()}_ROW_COUNT="
        f"{saved_dataframe.count()}"
    )
    saved_dataframe.printSchema()

def main() -> None:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("shipment-pulse-bronze-ingestion")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        ports_dataframe = (
            spark.read.schema(PORT_SCHEMA)
            .option("header", True)
            .option("mode", "FAILFAST")
            .csv(f"{LANDING_PATH}/ports/ports.csv")
        )

        shipments_dataframe = (
            spark.read.schema(SHIPMENTS_CDC_SCHEMA)
            .option("mode", "FAILFAST")
            .json(f"{LANDING_PATH}/shipments_cdc/batch_001.jsonl")
        )

        events_dataframe = (
            spark.read.schema(SHIPMENT_EVENTS_SCHEMA)
            .option("mode", "FAILFAST")
            .json(f"{LANDING_PATH}/shipment_events/batch_001.jsonl")
        )

        datasets = {
            "ports": ports_dataframe,
            "shipments_cdc": shipments_dataframe,
            "shipment_events": events_dataframe,
        }

        for dataset_name, dataframe in datasets.items():
            bronze_dataframe = add_ingestion_metadata(
                dataframe,
                dataset_name,
            )
            write_bronze_dataset(
                spark,
                dataset_name,
                bronze_dataframe,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()