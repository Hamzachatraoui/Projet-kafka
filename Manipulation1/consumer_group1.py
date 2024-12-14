from confluent_kafka import Consumer, KafkaException, KafkaError
import csv

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer-group1',
    'auto.offset.reset': 'earliest'  # Start consuming from the earliest message
}

consumer = Consumer(conf)

# Subscribe to the 'logs' topic
consumer.subscribe(['logs'])

# Open CSV files for writing WARN and INFO messages
with open('warn_logs.csv', mode='w', newline='', encoding='utf-8') as warn_file, \
     open('info_logs.csv', mode='w', newline='', encoding='utf-8') as info_file:

    warn_writer = csv.writer(warn_file)
    info_writer = csv.writer(info_file)

    # Write headers for CSV files
    warn_writer.writerow(['Timestamp', 'Level', 'Message'])
    info_writer.writerow(['Timestamp', 'Level', 'Message'])

    try:
        print('Listening for messages...')

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'Reached end of partition {msg.partition()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Decode the message value to a string
                message_value = msg.value().decode('utf-8')
                timestamp = msg.timestamp()[1]  # Get message timestamp

                # Check if the log level is WARN or INFO
                if 'WARN' in message_value:
                    warn_writer.writerow([timestamp, 'WARN', message_value])
                    warn_file.flush()  # Force write to file
                    print(f"[WARN] {message_value}")
                elif 'INFO' in message_value:
                    info_writer.writerow([timestamp, 'INFO', message_value])
                    info_file.flush()  # Force write to file
                    print(f"[INFO] {message_value}")

    except KeyboardInterrupt:
        print('Consumer interrupted')
    finally:
        # Close the consumer to free resources
        consumer.close()

