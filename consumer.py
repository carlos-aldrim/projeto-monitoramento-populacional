from kafka import KafkaConsumer
import json
import os

topics = ['ibge-topic', 'brasilapi-topic', 'correios-topic']

bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=bootstrap_servers,
    group_id='my-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"Consumidor iniciado, escutando tópicos: {topics}")

for msg in consumer:
    print(f"\nTópico: {msg.topic}")
    print(f"Mensagem: {msg.value}")
