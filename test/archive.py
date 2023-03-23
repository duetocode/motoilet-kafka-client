from motoilet_kafka_client import create_client
from confluent_kafka import Producer
import logging


def on_message(message: str, producer: Producer):
    print(message)
    raise Exception("Test")


def main():
    # setup the standard logging format
    logging.basicConfig(
        format="[%(asctime)s] %(name)s %(levelname)s  %(message)s",
        level=logging.DEBUG,
    )
    logger = logging.getLogger()

    logger.info("Welcone to the event archive program.")

    kafka_client = create_client(on_message)
    kafka_client.run()


if __name__ == "__main__":
    main()
