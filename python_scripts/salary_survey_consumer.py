from helper.postgres_helper import create_connection
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col

def create_tables(conn, curr):
    curr.execute("""
    CREATE TABLE IF NOT EXISTS earnings_per_job (
        JOB_GROUP VARCHAR(50),
        AVERAGE_SALARY FLOAT
    );""")

    curr.execute("""
    CREATE TABLE IF NOT EXISTS industry_earnings (
        INDUSTRY_GROUP VARCHAR(50),
        AVERAGE_SALARY FLOAT
    );""")

    conn.commit()

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


if __name__ == "__main__":
    conn = create_connection("localhost", "5432", "salary_survey_db", "salary_survey", "secret")
    curr = conn.cursor()

    create_tables(conn, curr)

    raw_stream = get_raw_stream("salary_survey_users", "salary_survey")

    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("AGE", IntegerType(), True),
        StructField("JOB", StringType(), True),
        StructField("INDUSTRY", StringType(), True),
        StructField("SALARY", FloatType(), True),
        StructField("INPUT_TIME", TimestampType(), True)
    ])
    
    parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    writeOut = parsed_stream.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()
    
    writeOut.awaitTermination()