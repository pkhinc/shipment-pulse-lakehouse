from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

BRONZE_PATH = "/opt/project/data/generated/bronze"

METADATA_COLUMNS = [
    "_ingested_at",
    "_ingestion_date",
    "_source_file",
    "_source_dataset",
    "_batch_id",
]

def validate_dataset(dataframe:DataFrame, dataset_name:str, expected_rows: int, timestamp_columns: list[str],) -> None:
    required_columns = METADATA_COLUMNS + timestamp_columns
    missing_columns = sorted(set(required_columns) - set(dataframe.columns))

    if missing_columns:
        raise ValueError(
            f"{dataset_name}: missing required columns: {missing_columns}"
        )

    row_count = dataframe.count()
    print(f"BRONZE_{dataset_name.upper()}: row count = {row_count}")

    if row_count != expected_rows:
        raise ValueError(
            f"{dataset_name}: expected {expected_rows} rows, but found {row_count}"
        )

    for column_name in timestamp_columns + ["_ingested_at"]:
        actual_type = dataframe.schema[column_name].dataType

        if not isinstance(actual_type, TimestampType):
            raise TypeError(
                f"{dataset_name}.{column_name}: "
                f"expected timestamp, found {actual_type}"
            )

    invalid_condition = F.lit(False)

    for column_name in required_columns:
        invalid_condition = (
            invalid_condition | F.col(column_name).isNull()
        )

    invalid_condition = (
        invalid_condition
        | (F.trim(F.col("_source_file")) == "")
        | (F.col("_source_dataset") != dataset_name)
        | (F.col("_batch_id") != "batch_001")
    )

    invalid_rows = dataframe.filter(invalid_condition).count()

    if invalid_rows > 0:
        raise ValueError(
            f"{dataset_name}: found {invalid_rows} rows "
            "have missing timestamp values or invalid metadata"
        )

    print(f"BRONZE_{dataset_name.upper()}_check = ok")

def main() -> None:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("shipment-pulse-bronze-validation")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    datasets = {
        "ports": (6, []),
        "shipments_cdc": (20, ["operation_timestamp", "planned_arrival_time", "planned_departure_time"]),
        "shipment_events": (20, ["event_time", "received_at"]),}

    try:
        for dataset_name, settings in datasets.items():
            expected_rows, timestamp_columns = settings

            dataframe = spark.read.parquet(f"{BRONZE_PATH}/{dataset_name}")
            validate_dataset(dataframe, dataset_name, expected_rows, timestamp_columns)

        print("Bronze validation completed successfully.")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()