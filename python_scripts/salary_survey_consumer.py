from helper.postgres_helper import ensure_table

if __name__ == "__main__":
    query = """
    CREATE TABLE IF NOT EXISTS users_salary (
        ID VARCHAR(50),
        NAME VARCHAR(50),
        AGE INT,
        JOB VARCHAR(50),
        SALARY DOUBLE PRECISION,
        INPUT_TIME TIMESTAMP    
    );
"""

    conn = ensure_table(query, "localhost", "5432", "salary_survey_db", "salary_survey", "secret")
