# Extract Module – BTCUSDT Stream Crawler to Kafka

This module is responsible for the **Extract** phase in the ETL pipeline for streamed analysis of BTCUSDT price data from the Binance trading platform. It fetches real-time price data and pushes it to a Kafka topic named `btc-price`.

## 📌 Features

- Fetches BTCUSDT price data from Binance REST API.
- Adds event-time information in ISO8601 format.
- Publishes messages to Kafka at least once every 100 milliseconds.
- Produces JSON messages to the `btc-price` Kafka topic.

## 🧩 Requirements

- Python 3.7+
- Kafka broker running on localhost
- Kafka topic `btc-price` (must be created before running)
- `kafka-python` library (not `kafka`)




# Report: [Link](https://docs.google.com/document/d/13DEzX0oV1MKGAN49JgAlZaQzvNxMX2xm/edit)