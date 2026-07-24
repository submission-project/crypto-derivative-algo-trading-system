import pytest

from messaging.consumer import KafkaConsumer


def test_kafka_consumer_normalizes_single_topic():
    consumer = KafkaConsumer(bootstrap_servers="localhost:9092", topic=" market.a ")

    assert consumer.topics == ("market.a",)


def test_kafka_consumer_normalizes_multiple_topics():
    consumer = KafkaConsumer(bootstrap_servers="localhost:9092", topic=(" market.a ", "market.b"))

    assert consumer.topics == ("market.a", "market.b")


def test_kafka_consumer_rejects_empty_topic_list():
    with pytest.raises(ValueError, match="at least one topic"):
        KafkaConsumer(bootstrap_servers="localhost:9092", topic=(" ", ""))
