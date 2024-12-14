import json
from confluent_kafka import Consumer, TopicPartition, KafkaException
from Connexion import Connexion

def consume_weather_data():
    # Kafka consumer configuration
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'marrakech_weather_group',
        'auto.offset.reset': 'earliest'
    }

    # Create a Kafka consumer instance
    consumer = Consumer(consumer_conf)

    topic = "morocco_weather"

    # Subscribe to partition 1 only
    consumer.assign([TopicPartition(topic, 1)])

    # Initialize database connection
    db_conn = Connexion()

    try:
        print(f"Consuming messages from topic {topic} on partition 1...")

        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print("End of partition reached")
                else:
                    print(f"Consumer error: {msg.error().str()}")
                continue

            try:
                # Decode Kafka message
                weather_data = json.loads(msg.value().decode('utf-8'))

                # Extract data
                city = weather_data.get('city')
                temperature = str(weather_data.get('temperature'))
                humidity = str(weather_data.get('humidity'))
                pressure = str(weather_data.get('pressure'))
                description = weather_data.get('weather_description')
                timestamp = weather_data.get('timestamp')

                # Insert data into MySQL
                db_conn.insert_weather_data(city, temperature, humidity, pressure, description, timestamp)

                print(f"Inserted data for {city} into MySQL")

            except Exception as parse_err:
                print(f"Failed to parse Kafka message or insert data: {msg.value()}, error: {parse_err}")

    except KeyboardInterrupt:
        print("Kafka consumer stopped.")

    finally:
        consumer.close()
        db_conn.close_db()


# Start consuming messages
if __name__ == "__main__":
    consume_weather_data()
