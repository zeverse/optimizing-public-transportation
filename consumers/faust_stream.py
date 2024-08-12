"""Defines trends calculations for stations"""

import logging

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


FAUST_INPUT_KAFKA_TOPIC = "pg_conn__stations"
FAUST_OUTPUT_KAFKA_TOPIC = "table__stations"
TABLE_NAME = ""

app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic(FAUST_INPUT_KAFKA_TOPIC, value_type=Station)
out_topic = app.topic(FAUST_OUTPUT_KAFKA_TOPIC, partitions=1)
table = app.Table(
    TABLE_NAME,
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#

@app.agent(topic)
def transformed_station(stations: list[Station]) -> None:
    for station in stations:
        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=station.red if station.red else station.blue if station.red else station.green,
        )


if __name__ == "__main__":
    app.main()
