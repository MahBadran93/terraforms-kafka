
# imports 
from kafka import KafkaProducer
import logging 
import json 
from sseclient import SSEClient
import requests
from datetime import datetime
import time 

# configs 
config = {
    'topic_name': 'wikimedia.recentchange',
    'bootstrap_servers': 'localhost:9092',
    'stream_url': 'https://stream.wikimedia.org/v2/stream/recentchange'}


class StreamURL:
    def __init__(self, stream_url) -> None:
        self.stream_url = stream_url

    def get_response_from_url(self):
        try:
            # response = requests.get(self.stream_url, stream=True)
            # client = sseclient.SSEClient(response)
            response = SSEClient(self.stream_url)
            return response
                
            
        except Exception as e:
            print(f"Error: {e}")
            

class WikiProducer: 

    def __init__(self, topic_name, message, producer) -> None:
        self.topic_name = topic_name
        self.producer = producer
        self.message = message


    def close(self):
        self.producer.close()


    def flush(self):
        self.producer.flush()


    def produce_message(self):
        # init a kafka producer 

        try:
            # init a time stamp
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

            if isinstance(self.message, dict):
                key = self.message['key']
                value = self.message['value']

                if key and value:
                    self.producer.send(self.topic_name, 
                                    key=self.message['key'],
                                    value=self.message['value']) 
                elif key == '':
                    self.producer.send(self.topic_name, 
                                    value=self.message['value']) 
            else: 
                record_metadata = self.producer.send(self.topic_name, value=self.message).get() 
                
                # log message to a file     
                logging.basicConfig(filename=f'./logs/kafka-logs-{timestamp}.log',
                                filemode='w', format='%(name)s - %(levelname)s - %(message)s')   
                # Set the logging level to INFO
                logging.getLogger().setLevel(logging.INFO) 
                logging.info(f"Message Metadata - Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            


        except Exception:
            logging.exception('error while producing a message')





stream = StreamURL(stream_url=config['stream_url'])
response = stream.get_response_from_url()
        
def test(response, timeout = 300):
    timeout_seconds = timeout # 5 min

    # Iterate over events
    producer = KafkaProducer(bootstrap_servers=config['bootstrap_servers'])

    
    try:
        # start time 
        start_time = time.time()  # Record the start time
        for event in response:
            if event.event == 'message':
                encoded_data = event.data.encode('utf-8')
                topic_producer = WikiProducer(topic_name=config['topic_name'], 
                                            message=encoded_data, producer=producer)
                topic_producer.produce_message()

            # break if reached timeout    
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout_seconds:
                print(f"Timeout reached ({timeout_seconds} seconds). Stopping.")
                break
            
    except requests.exceptions.ConnectionError:
        print("SSE stream closed or finished.")
        logging.error("SSE stream closed or finished.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Ensure to close the SSE client
        response.close()
        topic_producer.close()





test(response)
