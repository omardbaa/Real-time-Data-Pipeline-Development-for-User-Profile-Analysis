from confluent_kafka import Producer
import requests
import json
import time

topic = "user_profiles"
kafka_config = {
    "bootstrap.servers": "localhost:9092",  
}

producer = Producer(kafka_config)


max_retries = 100


batch_size = 5
batch_timeout = 10  

batch = []  
last_batch_time = time.time()  

while True:
    retry = 0
    while retry < max_retries:
        try:
            response = requests.get("https://randomuser.me/api/")
            data = json.dumps(response.json())
            batch.append(data)

            if len(batch) >= batch_size or time.time() - last_batch_time >= batch_timeout:
                
                producer.produce(topic, key="randomuser", value="\n".join(batch))
                producer.flush()
                batch = []  
                last_batch_time = time.time()  

            break  

        except requests.exceptions.ConnectTimeout:
            print(f"Retry {retry + 1}/{max_retries}: Connection timed out. Retrying...")
            retry += 1
            time.sleep(2)  
