from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col, avg, count

def get_raw_stream(app_name, topic):
    spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.2.23") \
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

def write_to_postgres(batch_df, batch_id):
    try:
        postgres_url = "jdbc:postgresql://localhost:5432/salary_survey_db"
        postgres_properties = {
            "user": "salary_survey",
            "password" : "secret",
            "driver" : "org.postgresql.Driver"
        }

        batch_df.write.jdbc(
            url=postgres_url,
            table="industry_earnings",
            mode="append",
            properties=postgres_properties
        )

        print(f"writing for batch {batch_id} succesfull!")
    except Exception as e:
        print(f"failed writing for {batch_id} due to: {e}")

def job_group_processing(rawstream, schema):
    parsed_stream = create_parsed_stream(rawstream, schema)

    job_group_df = parsed_stream.groupBy("JOB").agg(
        count("ID").alias("ENTRY_COUNT"),
        avg("SALARY").alias("AVERAGE_SALARY")
        )

    writeOut = job_group_df.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("update") \
            .start()
    
    writeOut.awaitTermination()

def industry_group_processing(rawstream, schema):
    parsed_stream = create_parsed_stream(rawstream, schema)

    industry_group_df = parsed_stream.groupBy("INDUSTRY").agg(
        count("ID").alias("ENTRY_COUNT"),
        avg("SALARY").alias("AVERAGE_SALARY")
        )

    writeOut = industry_group_df.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("update") \
            .start()
    
    writeOut.awaitTermination()