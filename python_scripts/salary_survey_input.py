from confluent_kafka import Producer
from datetime import datetime
from helper.kafka_helper import send_message
from helper.postgres_helper import quick_command

# Program specific functions: 

def get_numeric_input(message):
    while True:
        try:
            number_input = input(message)
            number_input = int(number_input)
        
            return number_input
        except:
            print("please insert a number")

            continue
        
def get_input():
    data = {}

    data['ID'] = input("Please input your id number: ")
    data['NAME'] = input("Please input your fullname: ")
    data['AGE'] = get_numeric_input("Please insert your age: ")
    data['JOB'] = input("Please insert your occupation")
    data['SALARY'] = get_numeric_input("Please insert your monthly salary in IDR: ") # Indonesian Rupiah

    data['INPUT_TIME'] = datetime.now()

    return data

if __name__ == "__main__":
    producer = Producer({"bootstrap.servers":"localhost:9092"}) # Input local machine port address
    
    data = get_input()
    # send_message(data, "salary_survey", producer)
    
    ensure_table_query = """
    CREATE TABLE IF NOT EXISTS users_salary (
        ID VARCHAR(50),
        NAME VARCHAR(50),
        AGE INT,
        JOB VARCHAR(50),
        SALARY DOUBLE PRECISION,
        INPUT_TIME TIMESTAMP    
    );
"""
    quick_command(ensure_table_query, "localhost", "5432", "salary_survey_db", "salary_survey", "secret")
    
    insert_to_postgres_query = """
    INSERT INTO users_salary (ID, NAME, AGE, JOB, SALARY, INPUT_TIME)
    VALUES (%s,%s,%s,%s,%s,%s)    
"""
    quick_command(insert_to_postgres_query, "localhost", "5432", "salary_survey_db", "salary_survey", "secret",
                  (data['ID'], data['NAME'], data['AGE'], data['JOB'], data['SALARY'], data['INPUT_TIME']))
    