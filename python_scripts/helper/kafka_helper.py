def get_input():
    data = {}

    data['id'] = '1'
    data['message'] = input("Please send any messages: ")

    return data

def delivery_report(err, msg):
    if err is not None:
        print(f"Message failed: {err}")
    else:
        print(f"message delivered to {msg.topic}, [{msg.partition()}]")


def send_message(topic, producer):
    data = get_input()

    producer.produce(
        topic,
        key='1',
        value=data['message'],
        on_delivery=delivery_report
    )

    producer.flush()