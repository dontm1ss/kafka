import json
from kafka import KafkaConsumer

def validate_message(data):
    required_fields = ["flight_number", "airline", "destination", "departure_time", "status", "gate", "timestamp"]
    
    for field in required_fields:
        if field not in data:
            return False
    
    valid_statuses = ["On Time", "Delayed", "Boarding", "Departed", "Cancelled"]
    if data["status"] not in valid_statuses:
        return False
    
    return True

consumer = KafkaConsumer(
    "airport",
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer запущен. Ожидание сообщений...")

for message in consumer:
    data = message.value
    
    if validate_message(data):
        print(f"{json.dumps(data, ensure_ascii=False)}")
    else:
        print(f"{json.dumps(data, ensure_ascii=False)} NOT VALID")