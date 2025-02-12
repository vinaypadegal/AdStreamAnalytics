from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, when, sum, max, lit
import time

spark = SparkSession \
    .builder \
    .appName("AdStreamAnalytics") \
    .master("local[4]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set('spark.sql.session.timeZone', 'UTC')
spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")

# PostgreSQL Connection Details
POSTGRES_URL = "jdbc:postgresql://localhost:5432/postgres"
POSTGRES_PROPERTIES = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver"
}

kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ad_events") \
        .option("startingOffsets", "earliest") \
        .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING)")

ad_df = json_df.selectExpr("CAST(value AS STRING) as json") \
    .selectExpr("json_tuple(json, 'event_id', 'timestamp', 'event_type', 'user_id', 'ad_id', 'campaign_id', 'cost_per_click') as (event_id, timestamp, event_type, user_id, ad_id, campaign_id, cost_per_click)")
ad_df = ad_df.withColumn("timestamp", round(col("timestamp"), 0).cast("timestamp"))
ad_df = ad_df.withColumn("cost_per_click", col("cost_per_click").cast("float"))


def write_to_postgres(batch_df, batch_id):
    print(f"Writing batch {batch_id} to PostgreSQL.")
    if batch_df is None or batch_df.isEmpty():
        return
    
    # print(f"batch_df Type: {type(batch_df)}")
    # batch_df.printSchema()
    # batch_df.show(5)
    
    campaign_agg = (
        batch_df.groupBy("campaign_id")
        .agg(
            count(when(col("event_type") == "click", True)).alias("campaign_clicks"),
            count(when(col("event_type") == "impression", True)).alias("campaign_impressions"),
            round(sum(when(col("event_type") == "click", col("cost_per_click")).otherwise(0)), 2).alias("campaign_cost"),        
            # max("timestamp").alias("timestamp")
        )
        .withColumn(
            "campaign_interactions",
            col("campaign_clicks") + col("campaign_impressions")
        )
        .withColumn(
            "campaign_ctr",
            round((col("campaign_clicks") / col("campaign_interactions")) * 100, 2)
        )
        .withColumn(
            "timestamp",
            round(lit(time.time()), 0).cast("timestamp")
        )
    )

    ad_agg = (
        batch_df.groupBy("campaign_id", "ad_id")
        .agg(
            count(when(col("event_type") == "click", True)).alias("ad_clicks"),
            count(when(col("event_type") == "impression", True)).alias("ad_impressions"),
            round(sum(when(col("event_type") == "click", col("cost_per_click")).otherwise(0)), 2).alias("ad_cost"),
            # max("timestamp").alias("timestamp")
        )
        .withColumn(
            "ad_interactions",
            col("ad_clicks") + col("ad_impressions")
        )
        .withColumn(
            "ad_ctr",
            round((col("ad_clicks") / col("ad_interactions")) * 100, 2)
        )
        .withColumn(
            "timestamp",
            round(lit(time.time()), 0).cast("timestamp")
        )
    )

    # Write to PostgreSQL (Append Mode)
    campaign_agg.write \
        .jdbc(url=POSTGRES_URL, table="ad_stream_analytics.campaign_metrics", mode="append", properties=POSTGRES_PROPERTIES)

    ad_agg.write \
        .jdbc(url=POSTGRES_URL, table="ad_stream_analytics.ad_metrics", mode="append", properties=POSTGRES_PROPERTIES)

    print(f"Batch {batch_id} written to PostgreSQL successfully!")


# Streaming Query (Processing every 5 seconds)
query = (
    ad_df.writeStream
    .outputMode("update")  # Emits only new aggregates per batch
    .foreachBatch(write_to_postgres)
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", "/tmp/spark-checkpoints")
    .start()
)

query.awaitTermination()