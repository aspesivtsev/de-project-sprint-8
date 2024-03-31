import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from config_settings import kafka_security_settings, postgresql_settings_docker, postgresql_settings_cloud, spark_jars_packages, topic_in, topic_out
from kafka.errors import KafkaError


#current time in miliseconds 
current_timestamp_utc = int(round(datetime.now(timezone.utc).timestamp()))

# writing data to PostgreSQL in Docker and Kafka
def foreach_batch_function(df):
    try:
        # we could use cache() here with the default settings actually but use persist() in case we need do some tweeks
        df.persist()

        # adding empty 'feedback' field
        feedback_df = df.withColumn('feedback', F.lit(None).cast(StringType()))

        # sending to PostgreSQL
        feedback_df.write.format('jdbc').mode('append') \
            .options(**postgresql_settings_docker).save()

        # serializing data for Kafka
        df_to_stream = (feedback_df
                    .select(F.to_json(F.struct(F.col('*'))).alias('value'))
                    .select('value')
                    )

        # sending to Kafka
        df_to_stream.write \
            .format('kafka') \
            .options(**kafka_security_settings) \
            .option('topic', topic_out) \
            .option('truncate', False) \
            .save()

        # clear memory from df
        df.unpersist()
        
    except Exception as e:
        # handling errors in Kafka
        print("Unable to connect to Kafka: ", e) 
   
def spark_init(spark_session_name) -> SparkSession:
    try:
        return (SparkSession
            .builder
            .appName({spark_session_name})
            .config("spark.jars.packages", spark_jars_packages)
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        )
    except Exception as e:
        print("An error occured during the creating of Spark session, please check your configuration parameters: ", e) 

# reading stream for restaurants
def restaurant_read_stream(spark):
    try:
        df = spark.readStream \
            .format('kafka') \
            .options(**kafka_security_settings) \
            .option('subscribe', topic_in) \
            .load()

        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("console") \
            .outputMode("append") \
            .start() \
            .awaitTermination()

        df_json = df.withColumn('key_str', F.col('key').cast(StringType())) \
            .withColumn('value_json', F.col('value').cast(StringType())) \
            .drop('key', 'value')

        # schema for incoming json message
        schema_of_incoming_message = StructType([
            StructField('restaurant_id', StringType(), nullable=True),
            StructField('adv_campaign_id', StringType(), nullable=True),
            StructField('adv_campaign_content', StringType(), nullable=True),
            StructField('adv_campaign_owner', StringType(), nullable=True),
            StructField('adv_campaign_owner_contact', StringType(), nullable=True),
            StructField('adv_campaign_datetime_start', LongType(), nullable=True),
            StructField('adv_campaign_datetime_end', LongType(), nullable=True),
            StructField('datetime_created', LongType(), nullable=True),
        ])

        # deserializing data from value and filtering by time
        df_string = df_json \
            .withColumn('key', F.col('key_str')) \
            .withColumn('value', F.from_json(F.col('value_json'), schema_of_incoming_message)) \
            .drop('key_str', 'value_json')

        df_filtered = df_string.select(
            F.col('value.restaurant_id').cast(StringType()).alias('restaurant_id'),
            F.col('value.adv_campaign_id').cast(StringType()).alias('adv_campaign_id'),
            F.col('value.adv_campaign_content').cast(StringType()).alias('adv_campaign_content'),
            F.col('value.adv_campaign_owner').cast(StringType()).alias('adv_campaign_owner'),
            F.col('value.adv_campaign_owner_contact').cast(StringType()).alias('adv_campaign_owner_contact'),
            F.col('value.adv_campaign_datetime_start').cast(LongType()).alias('adv_campaign_datetime_start'),
            F.col('value.adv_campaign_datetime_end').cast(LongType()).alias('adv_campaign_datetime_end'),
            F.col('value.datetime_created').cast(LongType()).alias('datetime_created'),
        ) \
            .filter((F.col('adv_campaign_datetime_start') <= current_timestamp_utc) & (
                    F.col('adv_campaign_datetime_end') > current_timestamp_utc))
        return df_filtered
    
    except Exception as e:
        # handling connection errors in Kafka
        print("Error connecting to Kafka: ", e)  

# getting all restaurant subscribers
def subscribers_restaurants(spark):
    try:
        df = spark.read \
            .format('jdbc') \
            .options(**postgresql_settings_cloud) \
            .load()
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("console") \
            .outputMode("append") \
            .start() \
            .awaitTermination()

        df = df.dropDuplicates(['client_id', 'restaurant_id'])
        return df
    except Exception as e:
        print("PostgreSQL data reading error occured, please check your configuration parameters!", e) 

#joining data on reastaurants and subscribers
def join(restaurant_read_stream_df, subscribers_restaurant_df):
    df = restaurant_read_stream_df \
        .join(subscribers_restaurant_df, 'restaurant_id') \
        .withColumn('trigger_datetime_created', F.lit(current_timestamp_utc)) \
        .select(
        'restaurant_id',
        'adv_campaign_id',
        'adv_campaign_content',
        'adv_campaign_owner',
        'adv_campaign_owner_contact',
        'adv_campaign_datetime_start',
        'adv_campaign_datetime_end',
        'datetime_created',
        'client_id',
        'trigger_datetime_created')
    return df

if __name__ == '__main__':
    spark = spark_init("RestaurantSubscribeStreamingService")
    print("\n Session created successfully \n")
    spark.conf.set('spark.sql.streaming.checkpointLocation', 'test_query')
    print("\n Checkpoint created \n")
    restaurant_read_stream_df = restaurant_read_stream(spark)
    print("\n Getting restaurant promos \n")
    subscribers_restaurant_df = subscribers_restaurants(spark)
    print("\n Defining users who have restaurants in the list of favorites \n")
    result = join(restaurant_read_stream_df, subscribers_restaurant_df)
    print("\n Lists for notifications are created successfully \n")

    query = (result.writeStream \
        .foreachBatch(foreach_batch_function) \
        .start())
    try:
        query.awaitTermination()
    finally:
        query.stop()
