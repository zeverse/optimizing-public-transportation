"""Configures a Kafka Connector for Postgres Station data"""

import json
import logging

import requests

from producers.config import ConnectorConfig


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    else:
        logging.debug("creating connector")
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "batch.max.rows": "500",
                    "connection.url": ConnectorConfig.url,
                    "connection.user": ConnectorConfig.user,
                    "connection.password": ConnectorConfig.password,
                    "table.whitelist": ConnectorConfig.table_whitelist,
                    "mode": ConnectorConfig.mode,
                    "incrementing.column.name": ConnectorConfig.incrementing_column,
                    "topic.prefix": ConnectorConfig.topic_prefix,
                    "poll.interval.ms": ConnectorConfig.poll_interval,
                },
            }
        ),
    )

    ## Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
