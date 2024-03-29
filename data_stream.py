import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date


# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), False),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), False),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

# TODO create a spark udf to convert time to YYYYmmDDhh format
@psf.udf(StringType())
def udf_convert_time(timestamp):
    return parse_date(timestamp).strftime("%Y%m%d%H")


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "service-calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetPerTrigger", 200) \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("SERVICE_CALLS"))\
        .select("SERVICE_CALLS.*")

    distinct_table = service_table\
        .select(psf.col('crime_id'),
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_datetime'),
                psf.col('address'),
                psf.col('disposition'))

    # TODO get different types of original_crime_type_name in 60 minutes interval
    counts_df = distinct_table \
        .withWatermark("call_datetime", "60 minutes") \
        .groupBy(
            psf.window(distinct_table.call_datetime, "60 minutes", "5 minutes"),
            distinct_table.original_crime_type_name) \
        .count()

    # TODO use udf to convert timestamp to right format on a call_date_time column
    converted_df = service_table.select(udf_convert_time(psf.col('call_date_time')).alias('call_datetime'))

    # TODO apply aggregations using windows function to see how many calls occurred in 2 day span
    calls_per_2_days = distinct_table \
        .withWatermark("call_datetime", "2 days") \
        .groupBy(psf.window(distinct_table.call_datetime, "2 days")) \
        .count()

    # TODO write output stream
    query1 = counts_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .start()
    
    query2 = converted_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    query3 = calls_per_2_days \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .start()

    # TODO attach a ProgressReporter
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Local mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Udacity") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()



