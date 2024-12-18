# kafka_config = {
#     "bootstrap_servers": ['77.81.230.104:9092'],
#     "username": 'admin',
#     "password": 'VawEzo1ikLtrA8Ug8THa',
#     "security_protocol": 'SASL_PLAINTEXT',
#     "sasl_mechanism": 'PLAIN'
# }

# topic_suffix = 'buhai'
# topic_name = f"building_sensors_{topic_suffix}"
# alert_topic = f"avg_alerts_{topic_suffix}"

# configs.py

kafka_config = {
    "bootstrap_servers": ['77.81.230.104:9092'],
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN'
}

topic_suffix = 'buhai'
topic_name = f"building_sensors_{topic_suffix}"
alert_topic = f"avg_alerts_{topic_suffix}"
