from kafka import KafkaConsumer
import json
from configs import kafka_config, alert_topic

consumer = KafkaConsumer(
    alert_topic,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='alert-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    key_deserializer=lambda x: x.decode('utf-8') if x else None
)

print(f"Listening to alerts on topic '{alert_topic}'...")

for message in consumer:
    print(f"Received alert: Key={message.key}, Value={message.value}")
