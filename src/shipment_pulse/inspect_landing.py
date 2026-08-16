from pyspark.sql import DataFrame, SparkSession

LANDING_PATH = "/opt/project/data/generated/landing"

def inspect_dataset(name: str, dataframe: DataFrame) -> None:
    print(f"\n={name}=")
    print(f"{name}_ROW_COUNT={dataframe.count()}")
    dataframe.printSchema()
    dataframe.show(3, truncate=False)

def main() -> None:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("shipment-pulse-landing-inspection")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("warn")

    try:
        ports_df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .option("mode", "FAILFAST")
            .csv(f"{LANDING_PATH}/ports/ports.csv")
        )

        shipments_df = (
            spark.read.option("mode", "FAILFAST")
            .json(f"{LANDING_PATH}/shipments_cdc/batch_001.jsonl")
        )

        events_df = (
            spark.read.option("mode", "FAILFAST")
            .json(f"{LANDING_PATH}/shipment_events/batch_001.jsonl")
        )

        inspect_dataset("PORTS", ports_df)
        inspect_dataset("SHIPMENTS", shipments_df)
        inspect_dataset("EVENTS", events_df)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()        