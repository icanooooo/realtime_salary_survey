import json

def delivery_report(err, msg):
    if err is not None:
        print(f"Message failed: {err}")
    else:
        print(f"message delivered to {msg.topic}, [{msg.partition()}]")


def send_message(data, topic, producer):
    producer.produce(
        topic,
        key=data['id'],
        value=json.dumps(data),
        on_delivery=delivery_report
    )

    producer.flush()