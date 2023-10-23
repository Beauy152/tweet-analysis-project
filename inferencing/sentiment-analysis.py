from kafka import KafkaProducer, KafkaConsumer
from transformers import pipeline,Pipeline,RobertaTokenizer, RobertaForSequenceClassification
import json
import logging
import torch
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

def get_producer(host:str):
    return KafkaProducer(bootstrap_servers=host,
                              value_serializer=lambda t: str(t).encode('utf-8'),
                              key_serializer= lambda k: str(k).encode('utf-8')
                              )

def get_consumer(host:str,topic:str) -> KafkaConsumer:
    return KafkaConsumer(
    topic,
    bootstrap_servers=host,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-group20',
    consumer_timeout_ms=10000
)

def get_sentiment_pipeline():
    tokenizer = RobertaTokenizer.from_pretrained("cardiffnlp/twitter-roberta-base-sentiment-latest")
    model = RobertaForSequenceClassification.from_pretrained("cardiffnlp/twitter-roberta-base-sentiment-latest")
    model.to('cuda')
    return tokenizer,model



def write_sentiment(sentiment,tweet,producer:KafkaProducer,topic:str,aggregator):
    label_map = ["negative", "neutral", "positive"]
    label = label_map[sentiment]

    producer.send(topic,value=label,key=tweet.get('id_str'))

    if label == 'positive':
        aggregator['pos'] += 1
    elif label == 'neutral':
        aggregator['neu'] += 1
    else:
        aggregator['neg'] += 1


def generate_sentiment(tweet,tokenizer,model):
    # Tokenize input and move to GPU
    inputs = tokenizer(tweet.get('text'), return_tensors="pt", truncation=True, padding=True, max_length=512).to('cuda')

    # Predict
    with torch.no_grad():
        logits = model(**inputs).logits
    
    # Get the predicted class (0, 1, or 2 for negative, neutral, positive)
    predicted_class = torch.argmax(logits, dim=1).item()
    
    # Return sentiment (you might want to map the class to a string label here)
    return predicted_class

def process_tweet(tweet,tokenizer,model,producer,topic,aggregator):
    # Perform Sentiment analysis on tweet.
    sentiment = generate_sentiment(tweet,tokenizer,model)
    # print(f"{sentiment[0].get('label')}:{round(sentiment[0].get('score'),2)} | {tweet.get('text')}")
    
    # Write sentiment
    write_sentiment(sentiment,tweet,producer,topic,aggregator) 

def compute_aggregates(aggregator:dict[str,int]):
    pos = aggregator['pos']
    neu = aggregator['neu']
    neg = aggregator['neg']
    total = pos + neu + neg
    pos_frac = pos / total if pos > 1 else 0
    neu_frac = neu / total if neu > 1 else 0
    neg_frac = neg / total if neg > 1 else 0

    print(f"""
Postive  Tweets: {pos_frac * 100}
Neutral  Tweets: {neu_frac * 100}
Negative Tweets: {neg_frac * 100}
Total    Tweets: {total}
    """)

def main():
    # consts
    BOOTSTRAP_SERVER = '192.168.86.29:9092'
    READ_TOPIC = 'raw-tweets'
    WRITE_TOPIC = 'tweets-sentiment'

    # Create Logger
    logger = get_logger('MAIN')
    logger.info(f'Cuda is {"available" if torch.cuda.is_available() else "unavailable"}')
 
    # Create sentiment analysis pipeline
    tokenizer,model = get_sentiment_pipeline()
    logger.info("Created Pipeline")
    # logger.info(f"Pipeline using device: {pipeline.model.device}")

    # Connect to Kafka broker (Consumer)
    consumer = get_consumer(BOOTSTRAP_SERVER,READ_TOPIC)
    logger.info("Created Consumer")

    # Connect to Kafka broker (Producer)
    producer = get_producer(BOOTSTRAP_SERVER)
    logger.info("Created Producer")

    # Create Aggregator
    aggregator = {'pos':1,'neu':1,'neg':1}

    # Iterate over each tweet in consumer.
    logger.info("Beginning Processing Tweets...")

    with tqdm(desc='processing...') as t:
        while True:
            messages = consumer.poll(timeout_ms=8000,max_records=500)
            if messages == {}: break

            for _,batch in messages.items():
                for raw_tweet in batch:
                    process_tweet(raw_tweet.value,tokenizer,model,producer,WRITE_TOPIC,aggregator)
                    t.update(1)

    logger.info("Completed Processing Tweets.")


    logger.info("Computed aggregates:")
    compute_aggregates(aggregator)


if __name__ == "__main__":
    main()