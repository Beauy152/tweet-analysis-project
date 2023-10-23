import multiprocessing 
from glob import glob
import bz2
import json 
import os
from dataclasses import asdict,dataclass
from tqdm import tqdm
from kafka import KafkaProducer
from random import randint
from datetime import datetime

TIMESTAMP_FORMAT = '%a %b %d %H:%M:%S %z %Y'

BOOTSTRAP_SERVER = 'localhost:29092'
TOPIC = 'raw-tweets'

ROOT_CORPUS_REPO = "/Users/tylerbeaumont/Documents/Masters-Data-Science/big_data/D_HD-Project/twitter-corpus/Raw/**/*.bz2"
PROCESSED_REPO   = "/Users/tylerbeaumont/Documents/Masters-Data-Science/big_data/D_HD-Project/twitter-corpus/Processed"
PROCESSED_REPO = "./test"

WRITE_TO_FILE = False
NUM_WORKERS = 6

# These definitions are used to define explicitly what should be stored from each raw tweet
@dataclass
class TweetUser:
    id:str
    name:str
    screen_name:str

@dataclass
class Tweet:
    created_at:str
    year:str
    month:str
    day:str
    hour:str
    minute:str
    id_str:str
    text:str
    timestamp_ms:str
    favorite_count:int
    retweet_count:int
    reply_count:int
    hashtags:list[str]
    user: TweetUser

    @classmethod
    def from_dict(cls, data):
        created_at = data.get('created_at')
        timestamp = datetime.strptime(created_at,TIMESTAMP_FORMAT)
        return cls(
            created_at = created_at,
            year = str(timestamp.year),
            month = str(timestamp.month),
            day = str(timestamp.day),
            hour = str(timestamp.hour),
            minute = str(timestamp.minute),
            id_str = data.get('id'),
            text = data.get('text'),
            timestamp_ms=data.get('timestamp_ms'),
            favorite_count=data.get('favorite_count'),
            retweet_count=data.get('retweet_count'),
            reply_count=data.get('reply_count'),
            hashtags=data['entities'].get('hashtags'),
            user=TweetUser(
                data['user'].get('id_str'),
                data['user'].get('name'),
                data['user'].get('screen_name')
            )
        )

def write_tweet_to_file(tweet:Tweet):
    """Given a processed tweet, write it to a file in the processed repo."""
    # Cehck if year folder exists, otherwise create
    file_path = f"{PROCESSED_REPO}/{tweet.year}/{tweet.month}/"
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    # Append tweet to file, create is not exists.
    with open(file_path + f"{tweet.day}.txt", "a") as f:
        f.write(f"{json.dumps(asdict(tweet))}\n")


def write_tweet_to_kafka(tweet:Tweet,kafka_producer:KafkaProducer,num_partitions:int):
    """Given a processed tweet, write it to a kafka topic."""
    if kafka_producer is None:
        print('Kafka producer is None-type - check configuration.')
        raise ValueError('Kafka producer is None-type - check configuration.')
    
    p = randint(0,num_partitions-1)

    kafka_producer.send(TOPIC,tweet,str(tweet.id_str),partition=p)


def write_tweet(tweet:Tweet,kafka,num_partitions:int):
    """Write tweet to either a file or kafka topic."""
    if WRITE_TO_FILE:
        write_tweet_to_file(tweet)
    else:
        write_tweet_to_kafka(tweet,kafka,num_partitions)


def process_raw_tweet(raw_tweet:dict,counters:dict[str,int]) -> Tweet | None:
    # Check if tweet is deleted, if so exclude it.
    if "delete" in raw_tweet.keys():
        counters['deleted'] += 1 
        return None
    
    # Exclude non-english twwets, as we haven't the means to process them.
    if raw_tweet.get("lang") != "en":
        counters['non_english'] += 1
        return None
    
    # Return transformed tweet
    return Tweet.from_dict(raw_tweet)

def process_zips(id:int,zip_paths:list[str],counters:dict[str,int],kafka,num_partitions:int) -> None:
    """For each zip file, we need to extract to a new repo"""    
    for path in tqdm(zip_paths,desc=f"Worker:{id}",total=len(zip_paths)):
        # Unzip
        try:
            file = bz2.BZ2File(path).read().splitlines()
        except:
            print(f"Worker:{id}| Failed to unzip file, skipping...:\n{path}")
            continue

        # Parse Json
        for raw_tweet in file:
            try:
                raw_tweet_json = json.loads(raw_tweet.decode('utf-8'))
                if (parsed_tweet := process_raw_tweet(raw_tweet_json,counters)) is not None:

                    write_tweet(parsed_tweet,kafka,num_partitions)
                    counters['valid'] += 1
            except:
                counters['non-parseable'] += 1
                


def get_all_bzips(repo_dir:str) -> list[str]:
    """Recursively fetch all bzip file paths under the corpus repo."""
    zip_files = glob(repo_dir,recursive=True)
    print(f"Found {len(zip_files)} .bz2 files. ")

    return zip_files

def print_tweet_statistics(id:int,counters:dict[str,int]) -> None:
    """Prints statistics about the tweets processed."""
    deleted = counters.get('deleted')
    non_eng = counters.get('non_english')
    nonparseable   = counters.get('non-parseable')
    valid   = counters.get('valid')
    try:
        valid_percentage = round(valid / (valid + deleted + non_eng),2)
    except ZeroDivisionError:
        valid_percentage = 0

    print(f"""-----------------------------------------------------
Worker ID : {id}
Excluded {deleted} deleted tweets.  
Excluded {non_eng} non-english tweets.
Excluded {nonparseable} non-parseable tweets.
Processed {valid} valid tweets.

Valid tweets make up {valid_percentage * 100}% of the data.
-----------------------------------------------------""")

def check_for_processed_repo():
    """Check folder exists, otherwise create"""
    if not os.path.exists(PROCESSED_REPO):
        os.makedirs(PROCESSED_REPO)


def split_lists(list_to_split:list, num_splits:int) -> list[list]:
    # Calculate the number of items per sublist
    items_per_sublist = len(list_to_split) // num_splits
    remainder = len(list_to_split) % num_splits

    sublists = []
    start = 0

    for i in range(num_splits):
        end = start + items_per_sublist + (1 if i < remainder else 0)
        sublists.append(list_to_split[start:end])
        start = end

    return sublists


def worker_function(worker_id:int,path_list:list[str]):
    counters:dict[str,int] = {
        'deleted':0,
        'non_english':0,
        'non-parseable':0,
        'valid':0
    }

    if WRITE_TO_FILE is True:
        kafka = None
    else:
        kafka_serialiser = lambda t: json.dumps(asdict(t)).encode('utf-8')
        kafka = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER,
                              value_serializer=kafka_serialiser,
                              key_serializer= lambda k: k.encode('utf-8')
                              )
        num_partitions = len(kafka.partitions_for(TOPIC))


    try:
        process_zips(worker_id,path_list,counters,kafka,num_partitions)
    except:
        print("Error processing zips")
    
    print_tweet_statistics(worker_id,counters)

def main():
    check_for_processed_repo()

    # Get all bzips
    zip_paths = get_all_bzips(ROOT_CORPUS_REPO)

    # Split into sublists
    zip_paths = split_lists(zip_paths, NUM_WORKERS)

    # Validate lengths of paths & 
    if len(zip_paths) != NUM_WORKERS:
        raise ValueError("The number of zip-groups to be processed doesn't match the number of thread-workers...")


    # Create a list of processes
    processes = []
    for i in range(NUM_WORKERS):
        process = multiprocessing.Process(target=worker_function, args=(i,zip_paths[i]))
        processes.append(process)

    # Start all the processes
    for process in processes:
        process.start()

    # Wait for all processes to finish
    for process in processes:
        process.join()



if __name__ == "__main__":
    main()