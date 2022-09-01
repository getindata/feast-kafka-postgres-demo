# Feast Kafka Postgres demo

## Overview

This is a working demo of using Feast in a sample business case with Postgres and Kafka data sources. It is based on [feast-demo](https://github.com/feast-dev/feast-demo)

## Setup

### Setting up Feast

Prerequisites:
- Python 3.8 
- running Kafka instance available at localhost:9092
- running [Kafka REST Proxy](https://docs.confluent.io/platform/current/kafka-rest/quickstart.html) instance available at localhost:8082


Install the necessary requirements

```
pip install -r requirements.txt
```

We have already set up a feature repository in [feature_repo/](feature_repo/). 

Run the demo
```
cd feature_repo/
python demo/get_features_demo_kafka.py
```
