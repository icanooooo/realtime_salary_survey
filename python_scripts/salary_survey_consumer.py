from helper.postgres_helper import quick_command

if __name__ == "__main__":
    users_summary_query = """
    CREATE TABLE IF NOT EXISTS users_summary (
        POKEMON VARCHAR(50)
"""

    quick_command(users_summary_query, "localhost", "5432", "salary_survey_db", "salary_survey", "secret")
