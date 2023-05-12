"""Raw events schemas."""

from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

subscription_events_schema = StructType(
    [
        StructField("event_type", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("timestamp", LongType(), False),
        StructField("customer_id", StringType(), True),
        StructField("revenue", DoubleType(), True),
    ]
)
