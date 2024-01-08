from kafka import KafkaProducer
import logging
import traceback
from datetime import datetime
from kafka import partitioner
import requests

TOPIC_NAME = 'topic1'
KAFKA_SERVER = 'localhost:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SASL_MECHANISM = 'PLAIN'

# connect to local host
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

# # connect to remote server with security
# producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
#                          security_protocol=SECURITY_PROTOCOL,
#                          sasl_mechanism=SASL_MECHANISM)


# Generate a timestamp for the log file name
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
result = 'message not sent'
for i in range(3):
    try:
        message = f'test-message-{i}'.encode('utf-8')
        key = f'key-{i}'.encode('utf-8')

        record_metadata = producer.send(TOPIC_NAME, key=key,
                                        value=message).get()  # pass messages as binary by using the 'b'

        logging.basicConfig(filename=f'./logs/kafka-logs-{timestamp}.log',
                            filemode='w', format='%(name)s - %(levelname)s - %(message)s')

        # Set the logging level to INFO
        logging.getLogger().setLevel(logging.INFO)

        logging.info(
            f"Message Metadata - Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

        print(f"Message Metadata - Topic: {record_metadata.topic}",
              f"Partition: {record_metadata.partition}",
              f"Offset: {record_metadata.offset}")
        result = 'message sent'
    except Exception as e:
        logging.basicConfig(filename=f'./logs/kafka-logs-{timestamp}.log', filemode='w',
                            format='%(name)s - %(levelname)s - %(message)s')
        logging.warning('This message will be logged on to a file')
        logging.exception('error while producing a message')
        # Notify the console about the error
        # Retrieve and log metadata

        print("An error occurred. Details have been logged in './logs/kafka-logs.log'.")

        # traceback.print_exc()

print(result)
producer.flush()

producer.close()