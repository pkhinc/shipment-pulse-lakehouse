from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("shipment-pulse-smoke-test")
        .getOrCreate()
    )

    try:
        row_count = spark.range(5).count()

        print(f"SPARK_VERSION={spark.version}")
        print(f"ROW_COUNT={row_count}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()