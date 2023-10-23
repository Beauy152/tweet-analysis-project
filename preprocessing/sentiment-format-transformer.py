"""
The purpose of this script is to read from a topic, transform the message format,
and write to an alternative topic. this was needed as i had decided to change the
structure of my data after sentiment analysis had already been performed.
"""


from kafka import KafkaConsumer, KafkaProducer
import json
from tqdm import tqdm


def convert_to_json(message) -> dict:
    """
    Convert message from <key>:<value> format into JSON format.
    """
    msg = str(message.value)
    tweet_id = message.key.decode('utf-8')

    return {
        "tweet_id":tweet_id,
        "sentiment":msg
    }

def main():
    # Define Kafka configuration
    BOOTSTRAP_SERVER = 'localhost:29092'
    READ_TOPIC = 'tweets-sentiment'
    WRITE_TOPIC = 'sentiment-tweets'

    # Create a Kafka consumer
    consumer = KafkaConsumer(
        READ_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVER,
        auto_offset_reset='earliest',  # start at the beginning of the topic
        value_deserializer=lambda x: x.decode('utf-8')  # deserialize messages from bytes to str
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        # key_serializer= lambda k: k.encode('utf-8'),
        value_serializer=lambda x: json.dumps(x).encode('utf-8')  # serialize JSON to bytes
    )

    try:
        with tqdm(desc="Processing...") as t:
            for message in consumer:
                transformed_msg = convert_to_json(message)

                producer.send(WRITE_TOPIC,key=message.key,value=transformed_msg)
                t.update(1)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
