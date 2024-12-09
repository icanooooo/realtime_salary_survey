from helper.postgres_helper import quick_command
from helper.kafka_helper import send_message
from datetime import datetime
from confluent_kafka import Producer
from helper.helper import ensure_table

import random
import requests
import simplejson as json
import time

def generate_name():
    response = requests.get('https://randomuser.me/api/?nat=AU')

    if response.status_code == 200:
        user_data = response.json()['results'][0]

        full_name = f"{user_data['name']['first']} {user_data['name']['last']}"

        return full_name
    
    else:
        return "No data fetched"

def generate_fake_data(n):
    data = {}
    jobs = ['Data Engineer', 'Account Executive', 'Content Writer', 'Software Engineer', 'Writer', 'Mathematician', 'Sales', 'Data Scientist', 'Trade Coordinator', 'Commercial Executive', 'Engineer (General)', 'Accountant', 'Consultant']
    industry = ['Finance', 'Technology', 'Advertising', 'Trading', 'Logistics', 'Agriculture', 'Government', 'Construction', 'Oil & Gas', 'Consultancy']

    data['ID'] = str(n + 1000)
    data['NAME'] = generate_name()
    data['AGE'] = random.randint(20, 55)
    data['JOB'] = random.choice(jobs)
    data['INDUSTRY'] = random.choice(industry)
    data['SALARY'] = random.uniform(1, 50) * 1000000 # Indonesian Rupiah
    data['INPUT_TIME'] = str(datetime.now())

    return data

if __name__ == "__main__":
    producer = Producer({"bootstrap.servers":"localhost:9092"}) # Input local machine port address

    ensure_table()

    insert_data = """
    INSERT INTO users_salary (ID, NAME, AGE, JOB, INDUSTRY, SALARY, INPUT_TIME)
    VALUES (%s,%s,%s,%s,%s,%s)    
"""

    for i in range(100):
        data = generate_fake_data(i)

        quick_command(insert_data, "localhost", "5432", "salary_survey_db", "salary_survey", "secret",
                    (data['ID'], data['NAME'], data['AGE'], data['JOB'], data['INDUSTRY'], data['SALARY'], data['INPUT_TIME']))
        
        
        send_message(data, 'salary_survey', producer)

        print(f"Succesfully added: {data['NAME']}")

        time.sleep(random.randint(1, 10))


