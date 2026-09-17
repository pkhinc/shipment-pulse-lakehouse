from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_PORTS_PATH = "/opt/project/data/generated/bronze/ports"
SILVER_PORTS_PATH = "/opt/project/data/generated/silver/ports"


def main() -> None:
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("shipment-pulse-silver-validation")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    ports_df = spark.read.parquet(BRONZE_PORTS_PATH)
    ports_df = ports_df.withColumn("port_code", F.upper(F.trim("port_code")))
    ports_df = ports_df.withColumn("country_code", F.upper(F.trim("country_code")))
    ports_df = ports_df.withColumn("port_name", F.trim("port_name"))
    ports_df = ports_df.withColumn("region", F.trim("region"))
    ports_df = ports_df.filter(F.col("port_code") != "")
    ports_df = ports_df.dropDuplicates()

    if ports_df.count() != ports_df.select("port_code").distinct().count():
        raise ValueError("Duplicate port codes found in the dataset.")

    ports_df.write.mode("overwrite").parquet(SILVER_PORTS_PATH)
    spark.stop()

if __name__ == "__main__":
    main()    