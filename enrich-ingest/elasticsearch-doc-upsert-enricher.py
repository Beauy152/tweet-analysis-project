from kafka import KafkaConsumer,TopicPartition
from elasticsearch import Elasticsearch
import json
import logging
from tqdm import tqdm

def get_logger(name:str) -> logging.Logger:
    """Create a logger with custom format"""
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(module)s - %(message)s"
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger

def get_consumer(host:str,topic:str,group='test') -> KafkaConsumer:
    return KafkaConsumer(
    topic,
    bootstrap_servers=host,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=group,
    consumer_timeout_ms=10000
)

def get_elastic_client(host:str,port:int) -> Elasticsearch:
    return Elasticsearch(hosts=[{'host': host, 'port': port,'scheme':'http'}])

def update_elastic_document(elastic_client:Elasticsearch,index:str,doc_id:str,new_fields:dict):
    response = elastic_client.update(
        index=index,
        id=doc_id,
        body={"doc":new_fields}
        )
    return response

def create_elastic_document(elastic_client:Elasticsearch,index:str,doc_id:str,doc:dict):
    response = elastic_client.index(
        index=index,
        id=doc_id,
        body=doc
        )
    return response


def main():

    # Begin pipeline - 
    ELASTIC_HOST,ELASTIC_PORT = 'localhost',9200
    ELASTIC_INDEX = 'raw-tweets'#'tweets'

    KAFKA_HOST = 'localhost:29092'
    KAFKA_TOPIC_RAW = 'raw-tweets'
    KAFKA_TOPIC_SENTIMENT,PARTITION_SENTIMENT = 'sentiment-tweets',0
    KAFKA_CONSUMER_GROUP = 'kafka-elastic-upserter'

    # Create Logger
    logger = get_logger('main')
    logger.info("Starting...")

    # Create Elasticsearch Client
    logger.info("Creating Elasticsearch Client...")
    elastic_client = get_elastic_client(ELASTIC_HOST,ELASTIC_PORT)

    # NOTE: Yes, this could be made far more efficient, requiring only inserts. Please see
    # the "issues" section of the report.

    # 1. Raw-tweet - read from kafka, write to elastic. tweet_id as doc_id.

    # 1.1 Create consumer (kafka - raw_tweets)
    logger.info(f"Creating Kafka Consumer Client for Topic {KAFKA_TOPIC_RAW}...")
    consumer_raw = get_consumer(KAFKA_HOST,KAFKA_TOPIC_RAW,KAFKA_CONSUMER_GROUP)

    # 2.2 Write to elastic.
    for raw_tweet in tqdm(consumer_raw,desc='writing raw-tweets...'):
        tweet_id = raw_tweet.value.get('id_str')
        create_elastic_document(elastic_client,ELASTIC_INDEX,tweet_id,raw_tweet.value)

    # 2. Sentiment - read from kafka, update to elastic. tweet_id as doc_id

    # 2.1 Create Consumber (kafka - tweet-sentiment)
    logger.info(f"Creating Kafka Consumer Client for Topic {KAFKA_TOPIC_SENTIMENT}...")
    consumer_sentiment = get_consumer(KAFKA_HOST,KAFKA_TOPIC_SENTIMENT,KAFKA_CONSUMER_GROUP)
    sentiment_partition = TopicPartition(KAFKA_TOPIC_SENTIMENT,0)
    logger.info(f"Reading from offset: {consumer_sentiment.committed(sentiment_partition)} of partiiton {PARTITION_SENTIMENT}")

    # 2.2 Update to Elastic
    for tweet_sentiment in tqdm(consumer_sentiment,desc='updating tweet sentiments...'):
        tweet_id = tweet_sentiment.value.get('tweet_id')
        sentiment = {"sentiment":tweet_sentiment.value.get('sentiment')}

        update_elastic_document(elastic_client,ELASTIC_INDEX,tweet_id,sentiment)

if __name__ == "__main__":
    main()