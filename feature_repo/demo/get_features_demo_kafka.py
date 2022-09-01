import json
import subprocess
import time
import uuid
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
import requests
from feast import FeatureStore
from requests import Response

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)


def load_config_file(path: str):
    with open(path, "r") as json_file:
        return json.load(json_file)


config = load_config_file("config-local.json")
demo_topic = 'traffic'

model_feature_vector = [
    "order_details:country",
    "order_details:platform",
    "user_traffic:event",
    "user_traffic:event_event_timestamp"
]

user_traffic_vector = [
    "user_traffic:event",
    "user_traffic:event_event_timestamp",
    "user_traffic:session_listing_page_views",
    "user_traffic:session_product_page_views",
    "user_traffic:session_photo_page_views",
]


def run_demo():
    initialize_feast()
    store = FeatureStore(repo_path=".")

    print("\n--- Historical features ---")
    print_historical_features(store)

    print("\n--- Online features ---")
    print_online_features(store)

    print("\n--- Get consumer ---")
    consumer_url = get_consumer()

    print("\n--- Simulate a Kafka topic ingestion to the online store ---")
    event_df = ingest_sample_record_to_online_store(store, consumer_url)

    print("\n--- Online features again with updated values from a stream push---")
    online_df = print_online_features(store,
                                   entity_rows=[{'user': int(row['user_id']), 'traffic': int(row['traffic_id'])} for
                                                index, row in
                                                event_df.iterrows()])

    print("\n--- Example of data analysis and visualization ---")
    visualize_data(online_df)


def initialize_feast():
    run_command('feast teardown')
    run_command('feast apply')
    run_command('CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S") && feast materialize-incremental $CURRENT_TIME')


def run_command(cmd):
    output = subprocess.getstatusoutput(cmd)
    status = output[0]
    split_content = '\n'.join(output[1].split('\n'))
    print(f"---COMMAND---")
    print(f"Command {cmd} ran with output status {status} and contents: \n{split_content}")
    if status != 0:
        raise Exception(f"Command status: {status}")
    return output


def print_historical_features(store: FeatureStore):
    historical_df = store.get_historical_features(
        entity_df=get_input_df(),
        features=model_feature_vector,
    ).to_df()
    print("Historical features:")
    print(historical_df.head())


def get_input_df():
    return pd.DataFrame.from_dict(
        {
            "user_id": [0, 1],
            "order_id": [213, 2085],
            "traffic_id": ['7894', '13287'],
            "timestamp": [
                datetime(2022, 4, 12, 10, 59, 42),
                datetime(2022, 4, 12, 8, 12, 10),
            ]
        }
    )


def print_online_features(store: FeatureStore, entity_rows=None):
    if entity_rows is None:
        entity_rows = [{"user": 0, "traffic": 7894}, {"user": 1, "traffic": 13287}]
    features = store.get_online_features(
        features=user_traffic_vector,
        entity_rows=entity_rows
    ).to_dict()
    online_df = pd.DataFrame.from_dict(features)
    print("Received online features")
    print(online_df.head())
    return online_df


def get_consumer():
    consumer_config = {
        "format": "avro",
        "auto.offset.reset": "earliest",
        "auto.commit.enable": "true",
        "consumer.request.timeout.ms": 2147483647
    }

    resp = requests.post(
        f"{config['kafka_rest_proxy']}/consumers/feast-example-{str(uuid.uuid4())}",
        data=json.dumps(consumer_config),
        headers={'Content-Type': 'application/vnd.kafka.json.v2+json'}
    )
    log_response(resp)
    internal_consumer_url = json.loads(resp.content.decode())['base_uri']
    external_consumer_url = internal_consumer_url.replace("kafkarestproxy-0.kafkarestproxy.confluent.svc.cluster.local",
                                                          "localhost")

    subscribed_topics_config = {"topics": [demo_topic]}
    log_response(requests.post(
        f"{external_consumer_url}/subscription",
        data=json.dumps(subscribed_topics_config),
        headers={'Content-Type': 'application/vnd.kafka.json.v2+json'}
    ))
    return external_consumer_url


def get_topic_contents(consumer_url, max_timeout_seconds=5):
    def object_hook(obj):
        _isoformat = obj.get('_isoformat')
        if _isoformat is not None:
            return datetime.fromisoformat(_isoformat)
        return obj

    msg_list = []
    timeout_seconds = 0
    while msg_list == [] and timeout_seconds < max_timeout_seconds:
        resp = requests.get(
            f"{consumer_url}/records",
            headers={'Accept': 'application/vnd.kafka.avro.v2+json'}
        )
        resp.raise_for_status()
        log_response(resp)

        msg_list = json.loads(resp.content.decode(), object_hook=object_hook)

        if msg_list == []:
            time.sleep(1)
            timeout_seconds += 1
            print(f"Waiting for records for {timeout_seconds} seconds")
        else:
            timeout_seconds = 0

    return msg_list


def log_response(resp: Response):
    print(f"HTTP request (url: {resp.request.url}, method: {resp.request.method}) returned {resp.status_code}")


def ingest_sample_record_to_online_store(store: FeatureStore, consumer_url):
    msg_list = []
    # Bug when using write_to_online_store more than once
    while True:
        new_msg_list = get_topic_contents(consumer_url)
        print(f'Received {len(new_msg_list)} messages')
        msg_list = msg_list + new_msg_list
        print(f'Total {len(msg_list)} messages')
        if len(new_msg_list) == 0:
            print(f'Stopped receiving messages')
            break

    items = msg_list[0]['value'].keys()
    msg_vector = {item: [msg['value'][item] for msg in msg_list] for item in items}
    event_df = pd.DataFrame.from_dict(msg_vector)
    event_df['event_event_timestamp'] = event_df['timestamp'] / 1000
    event_df['event_created_timestamp'] = event_df['timestamp'] / 1000
    event_df.pop('timestamp')
    print("Received Kafka events")
    print(event_df.head())
    store.write_to_online_store("user_traffic", event_df)
    log_response(requests.delete(consumer_url))
    return event_df


def visualize_data(online_df):
    order_df = online_df[online_df['event'] == 'order_page']
    cols = ['session_listing_page_views', 'session_product_page_views', 'session_photo_page_views']
    fig, axes = plt.subplots(nrows=1, ncols=len(cols), figsize=(5, 5))
    index = 0
    for col in cols:
        axes[index].hist(order_df[col], bins=range(min(order_df[col]), max(order_df[col])))
        axes[index].set_title(col)
        index += 1
    fig.tight_layout()
    plt.show()


if __name__ == "__main__":
    run_demo()
