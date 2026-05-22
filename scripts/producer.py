import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

def generate_flight_data():
    airlines = ["Aeroflot", "S7 Airlines", "Pobeda", "Utair", "Rossiya", "Ural Airlines"]
    destinations = ["Moscow", "Saint Petersburg", "Sochi", "Kazan", "Novosibirsk", "Vladivostok", "Kaliningrad"]
    statuses = ["On Time", "Delayed", "Boarding", "Departed", "Cancelled"]
    
    departure_time = (datetime.now() + timedelta(hours=random.randint(-2, 4))).isoformat()
    
    data = {
        "flight_number": f"SU{random.randint(100, 999)}",
        "airline": random.choice(airlines),
        "destination": random.choice(destinations),
        "departure_time": departure_time,
        "status": random.choice(statuses),
        "gate": random.choice(["A1", "A2", "B3", "B4", "C1", "C2", "D5"]),
        "timestamp": datetime.now().isoformat()
    }
    return data

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

topic = "airport"

print("Producer запущен. Отправка сообщений...")

for i in range(10):
    message = generate_flight_data()
    producer.send(topic, value=message)
    print(f"Отправлено: {json.dumps(message, ensure_ascii=False)}")
    time.sleep(2)

producer.flush()
producer.close()
print("Producer завершил работу.")