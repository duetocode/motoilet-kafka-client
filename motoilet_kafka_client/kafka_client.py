import logging
import os
from typing import Callable
from confluent_kafka import Consumer, Producer, Message

logger = logging.getLogger("MotoiletKafkaConsumer")


class MotoiletKafkaConsumer:
    def __init__(
        self,
        topic: str,
        group_id: str,
        bootstrap_servers: str,
        message_handler: Callable[[Message, Producer], None],
        auto_offset_reset: str = "latest",
        dead_letter_topic: str = None,
    ):
        self._topic = topic
        self._group_id = group_id
        self._bootstrap_servers = bootstrap_servers
        self._message_handler = message_handler
        self._auto_offset_reset = auto_offset_reset
        self._consumer = None
        self._producer = None
        self._keep_running = False
        self._dead_letter_topic = (
            dead_letter_topic if dead_letter_topic is not None else f"{topic}_dead"
        )

    def stop(self):
        self._keep_running = False
        self._consumer.close()

    def run(self):
        self._initialize()
        self._keep_running = True

        try:
            while self._keep_running:
                message = self._consumer.poll(1.0)

                # no message, just continue
                if message is None:
                    continue

                # check error
                if message.error():
                    # retry if the error is retriable
                    if message.error().retriable():
                        logger.info(f"Retriable error occured: {message.error().str()}")
                        self._initialize()
                        continue
                    else:
                        logger.error(f"Error occured: {message.error().str()}")
                        self._keep_running = False
                        raise message.error()

                # dispatch message
                try:
                    self._message_handler(message, self._producer)
                except Exception as e:
                    logger.error("Error occured while handling message %s", e)
                    # send to dead letter topic
                    self._producer.produce(
                        self._dead_letter_topic, message.value(), message.key()
                    )
                    self._producer.flush()
                    logger.debug("Message sent to dead letter topic")

                # commit
                self._consumer.commit()
        finally:
            self._keep_running = False
            self._close()

    def _initialize(self):
        logger.info("Initializing consumer and producer")
        self._consumer = self._get_consumer(self._consumer)
        self._producer = self._get_producer()
        logger.info("Consumer and producer initialized")

    def _close(self):
        logger.info("Disposing consumer and producer")
        self._consumer.close()
        logger.info("Consumer and producer disposed")

    def _get_consumer(self, previous: Consumer = None) -> Consumer:
        # safely close the previous consumer with safeguard try-catch
        if previous is not None:
            try:
                previous.close()
            except:
                logger.warn("Error occured while closing previous consumer, ignored")

        # Prepare the configuration
        cfg = {
            "bootstrap.servers": self._bootstrap_servers,
            "group.id": self._group_id,
            "auto.offset.reset": self._auto_offset_reset,
            "enable.auto.commit": False,
        }

        logger.info(f"Creating consumer with config: {cfg}.")

        # Create the consumer
        consumer = Consumer(cfg)
        consumer.subscribe([self._topic])

        logger.info(f"Subscribed to topic: {self._topic}")

        return consumer

    def _get_producer(self) -> Producer:
        # Create the producer
        return Producer({"bootstrap.servers": self._bootstrap_servers})


def create_producer() -> Producer:
    """Create a Producer with configuration from the environment variables."""
    return Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})


def create_client(
    message_handler: Callable[[bytes, Producer], None]
) -> MotoiletKafkaConsumer:
    """Create a MotoiletKafkaConsumer with configuration from the environment variables."""

    return MotoiletKafkaConsumer(
        topic=os.environ["KAFKA_TOPIC"],
        group_id=os.environ["KAFKA_CONSUMER_GROUP_ID"],
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        message_handler=message_handler,
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
    )
