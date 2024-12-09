from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col, avg

def get_raw_stream(app_name, topic):
    spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
    
    kafka_broker = "localhost:9092"
    kafka_topic = topic

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    return raw_stream

def create_parsed_stream(rawstream, schema):
    parsed_stream = rawstream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    return parsed_stream
    
def raw_stream_processing(rawstream, schema):
    parsed_stream = create_parsed_stream(rawstream, schema)

    writeOut = parsed_stream.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()
    
    writeOut.awaitTermination()

def job_group_processing(rawstream, schema):
    parsed_stream = create_parsed_stream(rawstream, schema)

    job_group_df = parsed_stream.groupBy("JOB").agg(avg("SALARY").alias("AVERAGE_SALARY"))

    writeOut = job_group_df.writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()
    
    writeOut.awaitTermination()

def industry_group_processing(rawstream, schema):
    parsed_stream = create_parsed_stream(rawstream, schema)

    industry_group_df = parsed_stream.groupBy("INDUSTRY").agg(avg("SALARY").alias("AVERAGE_SALARY"))

    writeOut = industry_group_df.writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()
    
    writeOut.awaitTermination()