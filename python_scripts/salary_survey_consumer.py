from helper.postgres_helper import create_connection
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType
from helper.spark_helper import get_raw_stream, raw_stream_processing, job_group_processing, industry_group_processing

def create_tables(conn, curr):
    curr.execute("""
    CREATE TABLE IF NOT EXISTS earnings_per_job (
        JOB_GROUP VARCHAR(50),
        AVERAGE_SALARY FLOAT
    );""")

    curr.execute("""
    CREATE TABLE IF NOT EXISTS industry_earnings (
        INDUSTRY VARCHAR(50),
        ENTRY_COUNT INT,
        AVERAGE_SALARY FLOAT
    );""")

    conn.commit()

if __name__ == "__main__":
    conn = create_connection("localhost", "5432", "salary_survey_db", "salary_survey", "secret")
    curr = conn.cursor()

    create_tables(conn, curr)

    raw_stream, spark = get_raw_stream("salary_survey_users", "salary_survey")

    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("AGE", IntegerType(), True),
        StructField("JOB", StringType(), True),
        StructField("INDUSTRY", StringType(), True),
        StructField("SALARY", FloatType(), True),
        StructField("INPUT_TIME", TimestampType(), True)
    ])

    industry_group_processing(raw_stream, schema, spark)