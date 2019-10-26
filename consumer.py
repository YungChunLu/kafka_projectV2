import logging
import pykafka
from pykafka.common import OffsetType

logging.basicConfig(format='%(message)s', level=logging.DEBUG)

if __name__ == "__main__":
    client = pykafka.KafkaClient(hosts="127.0.0.1:9092")
    consumer = client.topics[b'service-calls'].get_simple_consumer(
        auto_offset_reset=OffsetType.EARLIEST,
        reset_offset_on_start=True
    )
    for message in consumer:
        if message is not None:
            logging.info(message.value)