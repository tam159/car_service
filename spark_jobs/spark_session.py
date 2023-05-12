"""Spark session helper."""

from data_config import GCS_BUCKET, PROJECT_ID
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()
conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
conf.set("spark.files.overwrite", "true")
conf.set("parentProject", PROJECT_ID)
conf.set("temporaryGcsBucket", GCS_BUCKET)

spark = SparkSession.builder.config(conf=conf).appName("spark-jobs").getOrCreate()
