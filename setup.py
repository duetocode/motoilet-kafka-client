from setuptools import setup

setup(
    name="motoilet-kafka-client",
    version="0.4.1",
    description="Motoilet Kafka Client",
    author="Liang",
    packages=["motoilet_kafka_client", "motoilet_logging"],
    install_requires=[
        "confluent-kafka~=2.0.2",
    ],
)
