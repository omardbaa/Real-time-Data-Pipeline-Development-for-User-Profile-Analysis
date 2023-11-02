from confluent_kafka import Producer
import requests
import json
import time

topic = "user_profiles"
kafka_config = {
    "bootstrap.servers": "localhost:9092",  # Change this to your Kafka server address
}

producer = Producer(kafka_config)

# Number of retry attempts
max_retries = 100

# Define batch size and time interval
batch_size = 1 
batch_timeout = 10  

batch = []  # List to accumulate records for batching
last_batch_time = time.time()  # Timestamp for the last batch

while True:
    retry = 0
    while retry < max_retries:
        try:
            response = requests.get("https://randomuser.me/api/")
            data = json.dumps(response.json())
            batch.append(data)

            if len(batch) >= batch_size or time.time() - last_batch_time >= batch_timeout:
                # If the batch size is reached or time interval is exceeded, send the batch
                producer.produce(topic, key="randomuser", value="\n".join(batch))
                producer.flush()
                batch = []  # Reset the batch
                last_batch_time = time.time()  # Update the timestamp

            break  # If successful, exit the retry loop

        except requests.exceptions.ConnectTimeout:
            print(f"Retry {retry + 1}/{max_retries}: Connection timed out. Retrying...")
            retry += 1
            time.sleep(2)  # 
