from confluent_kafka import Producer
import time

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'log-producer'
}

producer = Producer(conf)

# Callback to confirm message delivery
def delivery_report(err, msg):
    if err:
        print(f"Delivery Error: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to determine partition based on log level
def get_partition(log_level):
    if log_level == "ERROR":
        return 0
    elif log_level == "DEBUG":
        return 1
    elif log_level == "WARN":
        return 2
    elif log_level == "INFO":
        return 3
    else:
        return 3  # Default to INFO if log level is unrecognized

log_file_path = "/home/hamza/Documents/projetkafka/application.log"

# Continuously send logs to Kafka topic 'logs' with partitions
while True:
    try:
        with open(log_file_path, "r") as file:
            for line in file:
                line = line.strip()
                
                # Determine log level from log line
                if line.startswith("[ERROR]"):
                    log_level = "ERROR"
                elif line.startswith("[DEBUG]"):
                    log_level = "DEBUG"
                elif line.startswith("[WARN]"):
                    log_level = "WARN"
                elif line.startswith("[INFO]"):
                    log_level = "INFO"
                else:
                    log_level = "INFO"

                partition = get_partition(log_level)

                # Send logs to a specific partition
                producer.produce("logs", key=None, value=line.encode('utf-8'), partition=partition, callback=delivery_report)
                print(f"Sent log to partition {partition}: {line}")

                time.sleep(4)  # Pause to simulate a real-time log ingestion

        producer.flush(1)

    except FileNotFoundError:
        print(f"Log file {log_file_path} not found.")
        time.sleep(5)  # Wait before retrying in case the log file is created

