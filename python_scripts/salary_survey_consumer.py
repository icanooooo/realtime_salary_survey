from helper.postgres_helper import ensure_table

if __name__ == "__main__":
    query = """
    CREATE TABLE IF NOT EXISTS users_salary (
        id VARCHAR(50),
        name VARCHAR(50),
        age REAL,
        job VARCHAR(50),
        salary DOUBLE PRECISION,
        input_time TIMESTAMP    
    );
"""

    conn = ensure_table(query, "localhost", "5432", "salary_survey_db", "salary_survey", "secret")
