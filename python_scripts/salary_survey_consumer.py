from helper.postgres_helper import quick_command, create_connection

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

if __name__ == "__main__":
    conn = create_connection("localhost", "5432", "salary_survey_db", "salary_survey", "secret")
    curr = conn.cursor()

    create_tables(conn, curr)