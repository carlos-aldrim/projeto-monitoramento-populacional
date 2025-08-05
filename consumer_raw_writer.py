from kafka import KafkaConsumer
import json
import os
import time
from datetime import datetime
import uuid

MAX_RAW_FILES   = int(os.getenv('MAX_RAW_FILES', 0)) or None
RAW_RUN_SECONDS = int(os.getenv('RAW_RUN_SECONDS', 0)) or None

RAW_PATH = 'datalake/raw/'
os.makedirs(RAW_PATH, exist_ok=True)

topics = ['ibge-topic', 'brasilapi-topic', 'correios-topic']
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

group_id = f"raw-writer-{uuid.uuid4()}"

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=bootstrap_servers,
    group_id=group_id,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"[raw-writer] Consumidor iniciado, tópicos: {topics}, grupo: {group_id}")
print(f"[raw-writer] Limite de arquivos: {MAX_RAW_FILES}, tempo máximo: {RAW_RUN_SECONDS}s")

count = 0
start_time = time.time()

for msg in consumer:
    if MAX_RAW_FILES and count >= MAX_RAW_FILES:
        print(f"[raw-writer] Atingiu limite de {MAX_RAW_FILES} arquivos, encerrando.")
        break

    if RAW_RUN_SECONDS and (time.time() - start_time) > RAW_RUN_SECONDS:
        print(f"[raw-writer] Tempo máximo de {RAW_RUN_SECONDS}s atingido, encerrando.")
        break

    ts = datetime.now().strftime('%Y%m%d%H%M%S%f')
    filename = f"{msg.topic}_{ts}.json"
    filepath = os.path.join(RAW_PATH, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(msg.value, f, ensure_ascii=False)

    count += 1
    print(f"[raw-writer] Mensagem salva em: {filepath} (total: {count})")

print("[raw-writer] Finalizando consumidor.")  
