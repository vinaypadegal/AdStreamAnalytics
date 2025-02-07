from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession \
    .builder \
    .appName("AdStreamAnalytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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

campaign_clicks = ad_df.filter(ad_df.event_type == "click") \
    .groupBy("campaign_id") \
    .agg({"event_id": "count"}) \
    .withColumnRenamed("count(event_id)", "total_campaign_clicks")


campaign_clicks.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="1 second") \
    .start()

spark.streams.awaitAnyTermination()


# query = kafka_df \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .trigger(processingTime="1 second") \
#     .start()

# query.awaitTermination()