from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, count, round, when

spark = SparkSession \
    .builder \
    .appName("AdStreamAnalytics") \
    .master("local[4]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "5")

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
ad_df = ad_df.withColumn("timestamp", ad_df["timestamp"].cast("timestamp"))
ad_df = ad_df.withColumn("cost_per_click", ad_df["cost_per_click"].cast("float"))

# METRIC: Number of clicks per ad campaign
campaign_clicks = ad_df.filter(ad_df.event_type == "click") \
    .groupBy("campaign_id") \
    .agg({"event_id": "count"}) \
    .withColumnRenamed("count(event_id)", "total_campaign_clicks")

# METRIC: Click-through-rate at ad level
ad_ctr = (
    ad_df.groupBy("ad_id", "campaign_id")
    .agg(
        count("event_id").alias("total_interactions"),
        count(when(col("event_type") == "click", True)).alias("total_clicks")
        # count(when(col("event_type") == "impression", True)).alias("total_impressions")
    )
    .withColumn(
        "ad_ctr", 
        round((col("total_clicks") / col("total_interactions")) * 100, 2)
    )
)

# METRIC: Click-through rate at campaign level
campaign_ctr = (
    ad_df.groupBy("campaign_id")
    .agg(
        count("event_id").alias("total_interactions"),
        count(when(col("event_type") == "click", True)).alias("total_clicks")
    )
    .withColumn(
        "campaign_ctr", 
        round((col("total_clicks") / col("total_interactions")) * 100, 2)
    )
)


query_campaign_clicks = (
    campaign_clicks.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()
)

query_ad_ctr = (
    ad_ctr.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()
)

query_campaign_ctr = (
    campaign_ctr.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()
)

query_campaign_clicks.awaitTermination()
query_ad_ctr.awaitTermination()
query_campaign_ctr.awaitTermination()

# spark.streams.awaitAnyTermination()





# query = kafka_df \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .trigger(processingTime="1 second") \
#     .start()

# query.awaitTermination()


# METRIC: Click-through-rate at ad level
# ad_ctr = (
#     ctr_df
#     .withColumn(
#         "ad_ctr", 
#         round((col("total_clicks") / col("total_interactions")) * 100, 2)
#     )
# )

# METRIC: Click-through rate at campaign level
# campaign_ctr = (
#     ctr_df.groupBy("campaign_id")
#     .agg(
#         sum("total_clicks").alias("total_campaign_clicks"),
#         sum("total_interactions").alias("total_campaign_interactions")
#     )
#     .withColumn(
#         "campaign_ctr", 
#         round((col("total_campaign_clicks") / col("total_campaign_interactions")) * 100, 2)
#     )
# )