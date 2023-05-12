"""Streaming subscription_events data from Confluent Kafka to BigQuery."""

from data_config import GCS_BUCKET, PROJECT_ID, RAW_DATASET
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StringType
from schemas.raw_events import subscription_events_schema
from spark_libs import SparkKafka
from spark_session import spark

TOPIC_NAME = "subscription_events"

spark_kafka = SparkKafka(spark)


def ingest_subscription_events(dataframe: DataFrame) -> DataFrame:
    """
    Ingest subscription_events data then convert the timestamp.

    :param dataframe: DataFrame
    :return: extracted subscription_events DataFrame
    """
    dataframe = (
        dataframe.withColumn("value", col("value").cast(StringType()))
        .select(from_json("value", subscription_events_schema).alias("extracted_value"))
        .select("extracted_value.*")
    )
    dataframe = dataframe.withColumn("timestamp", to_timestamp(dataframe["timestamp"]))

    return dataframe


if __name__ == "__main__":
    df = spark_kafka.read_stream(topic_name=TOPIC_NAME)
    subscription_events_df = ingest_subscription_events(dataframe=df)

    spark_kafka.write_stream(
        dataframe=subscription_events_df,
        table_name=f"{PROJECT_ID}.{RAW_DATASET}.{TOPIC_NAME}",
        checkpoint_location=f"gs://{GCS_BUCKET}/checkpoint/{TOPIC_NAME}",
    )
