# import uuid
# import time
# from datetime import datetime
# import random
# from configs import kafka_config, topic_name
# from kafka import KafkaProducer
# import json

# producer = KafkaProducer(
#     bootstrap_servers=kafka_config['bootstrap_servers'],
#     security_protocol=kafka_config['security_protocol'],
#     sasl_mechanism=kafka_config['sasl_mechanism'],
#     sasl_plain_username=kafka_config['username'],
#     sasl_plain_password=kafka_config['password'],
#     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#     key_serializer=lambda v: json.dumps(v).encode('utf-8')
# )


# sensor_id = random.randint(1, 5)

# for i in range(99999):
#     try:
#         data = {
#             "sensor_id": sensor_id,
#             "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#             "temperature": random.randint(25, 45),
#             "humidity": random.randint(15, 85)
#         }
        
#         producer.send(topic_name, key=str(uuid.uuid4()), value=data)
#         producer.flush()
#         print(f"Message {i} with values {data} sent to topic '{topic_name}' successfully.")
#         time.sleep(2)
#     except Exception as e:
#         print(f"An error occurred: {e}")

# producer.close()

# sensor_producer.py

import uuid
import time
from datetime import datetime
import random
from configs import kafka_config, topic_name
from kafka import KafkaProducer
import json
import argparse

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def generate_sensor_data(sensor_id):
    data = {
        "sensor_id": sensor_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": random.randint(25, 45),
        "humidity": random.randint(15, 85)
    }
    return data

def main(num_sensors, interval):
    producer = create_producer()
    sensor_ids = [random.randint(1, 1000) for _ in range(num_sensors)]

    try:
        i = 0
        while True:
            for sensor_id in sensor_ids:
                data = generate_sensor_data(sensor_id)
                producer.send(topic_name, key=str(uuid.uuid4()), value=data)
                print(f"Message {i} from Sensor {sensor_id} sent to topic '{topic_name}': {data}")
                i += 1
                time.sleep(interval)
    except KeyboardInterrupt:
        print("Stopping sensor data generation.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka Sensor Data Producer')
    parser.add_argument('--num_sensors', type=int, default=1, help='Number of sensor instances to simulate')
    parser.add_argument('--interval', type=int, default=2, help='Interval between messages in seconds')
    args = parser.parse_args()
    main(args.num_sensors, args.interval)
