import pathlib
import json
import logging
import pykafka
import time

INPUT_FILE = 'police-department-calls-for-service.json'

logging.basicConfig(format='%(message)s', level=logging.DEBUG)


def read_file() -> json:
    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)
    return data


def generate_data(producer) -> None:
    data = read_file()
    for i in data:
        message = dict_to_binary(i)
        producer.produce(message)
        time.sleep(2)


# TODO complete this function
def dict_to_binary(json_dict: dict) -> bytes:
    """
    Encode your json to utf-8
    :param json_dict:
    :return:
    """
    return json.dumps(json_dict).encode('UTF-8')

# TODO set up kafka client
if __name__ == "__main__":
    client = pykafka.KafkaClient(hosts="127.0.0.1:9092")
    logging.info("topics", client.topics)
    producer = client.topics[b'service-calls'].get_producer()

    generate_data(producer)
