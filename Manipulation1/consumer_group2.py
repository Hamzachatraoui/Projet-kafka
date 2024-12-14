from confluent_kafka import Consumer, KafkaException, KafkaError
import csv
from datetime import datetime

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'group2',
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer
consumer = Consumer(conf)
consumer.subscribe(['logs'])

# Open CSV files for writing INFO, DEBUG, and WARN logs
with open('info_logs2.csv', mode='w', newline='', encoding='utf-8') as info_file, \
     open('debug_logs.csv', mode='w', newline='', encoding='utf-8') as debug_file, \
     open('error_logs.csv', mode='w', newline='', encoding='utf-8') as warn_file:

    # Define CSV writers
    info_writer = csv.writer(info_file)
    debug_writer = csv.writer(debug_file)
    warn_writer = csv.writer(warn_file)

    # Write headers to each CSV file
    info_writer.writerow(['Timestamp', 'Level', 'Message'])
    debug_writer.writerow(['Timestamp', 'Level', 'Message'])
    warn_writer.writerow(['Timestamp', 'Level', 'Message'])

    try:
        print('Listening for messages...')

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition')
                else:
                    raise KafkaException(msg.error())
            else:
                # Decode the message value to a string
                message_value = msg.value().decode('utf-8')

                # Get the current timestamp
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Determine the log level and write to the appropriate CSV file
                if '[INFO]' in message_value:
                    info_writer.writerow([timestamp, 'INFO', message_value])
                    info_file.flush()  # Force writing to disk
                    print(f"[INFO] {message_value}")

                elif '[DEBUG]' in message_value:
                    debug_writer.writerow([timestamp, 'DEBUG', message_value])
                    debug_file.flush()  # Force writing to disk
                    print(f"[DEBUG] {message_value}")

                elif '[ERROR]' in message_value:
                    warn_writer.writerow([timestamp, 'WARN', message_value])
                    warn_file.flush()  # Force writing to disk
                    print(f"[ERROR] {message_value}")

    except KeyboardInterrupt:
        print('Consumer interrupted')

    finally:
        # Close the consumer to free resources
        consumer.close()

