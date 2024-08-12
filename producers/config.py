from dataclasses import dataclass
from confluent_kafka.avro import CachedSchemaRegistryClient

BROKER_PROPERTIES = {
    "bootstrap.servers": [
        "PLAINTEXT://localhost:9092",
        "PLAINTEXT://localhost:9093",
        "PLAINTEXT://localhost:9094"
    ]
}
SCHEMA_REGISTRY = CachedSchemaRegistryClient({"url": "http://localhost:8081"})


class BrokerConfig:
    broker_properties = BROKER_PROPERTIES
    schema_registry: CachedSchemaRegistryClient = SCHEMA_REGISTRY


@dataclass
class ConnectorConfig:
    url: str = "jdbc:postgresql://localhost:5432/cta"
    user: str = "cta_admin"
    password: str = "chicago"
    table_whitelist: str = "stations"
    mode: str = "incrementing"
    incrementing_column: str = "stop_id"
    topic_prefix: str = "pg_conn__"
    poll_interval: int = 10000
