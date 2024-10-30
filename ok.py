from confluent_kafka import Consumer, KafkaError, KafkaException
import simplejson as json
def create_kafka_consumer(topic):
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'voting-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    return consumer

def fetch_data_from_kafka(consumer):
    data = []
    try:
        #while True:
        msg = consumer.poll(timeout=1.0)
        print(msg)
        if msg is None:
            return data
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return "Error"
            else:
                print(f"Consumer error: {msg.error()}")
                return data
        else:
            data.append(json.loads(msg.value().decode('utf-8')))
    except KafkaException as e:
        print(f"KafkaException: {e}")
    return data

if __name__ == "__main__":
    consumer = create_kafka_consumer("aggregated_votes_per_candidate")
    data = fetch_data_from_kafka(consumer)
    print(f"Fetched data: {data}")

    if not data:
        print("No data fetched from Kafka.")
    else:
        print(f"Data received: {data}")
