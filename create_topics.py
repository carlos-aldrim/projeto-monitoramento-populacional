from kafka.admin import KafkaAdminClient, NewTopic
import os

bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

topics = [
    NewTopic(name="ibge-topic", num_partitions=1, replication_factor=1),
    NewTopic(name="brasilapi-topic", num_partitions=1, replication_factor=1),
    NewTopic(name="correios-topic", num_partitions=1, replication_factor=1),
]

def create_topics():
    existing_topics = admin_client.list_topics()
    to_create = [t for t in topics if t.name not in existing_topics]
    if to_create:
        admin_client.create_topics(new_topics=to_create, validate_only=False)
        print(f"Tópicos criados: {[t.name for t in to_create]}")
    else:
        print("Tópicos já existem.")

if __name__ == "__main__":
    create_topics()
