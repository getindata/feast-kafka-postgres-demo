from datetime import timedelta

from feast import Entity, FeatureView, Field
from feast import KafkaSource
from feast.data_format import JsonFormat
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource
from feast.types import *

# --- ENTITIES ---

user = Entity(name="user", value_type=ValueType.INT64, join_keys=["user_id"], description="user id", )
order = Entity(name="order", value_type=ValueType.INT64, join_keys=["order_id"], description="order id", )
traffic = Entity(name="traffic", value_type=ValueType.INT64, join_keys=["traffic_id"], description="traffic id", )

# --- DATA SOURCES ---

orders = PostgreSQLSource(
    name="orders",
    query="""
             SELECT
               o.user_id    user_id,
               o.order_id   order_id,
               to_timestamp(o.timestamp/1000)  user_event_timestamp,
               to_timestamp(o.timestamp/1000)  user_created_timestamp,
               u.country    country,
               u.platform   platform
             FROM
             (SELECT user_id, order_id, timestamp FROM orders) o
             JOIN (SELECT user_id, country, platform FROM users) u
             ON o.user_id = u.user_id
                """,
    timestamp_field="user_event_timestamp",
    created_timestamp_column="user_created_timestamp",
)

batch_traffic = PostgreSQLSource(
    name="batch_traffic",
    query="""
             SELECT
               user_id   user_id,
               ''   event,
               to_timestamp(timestamp/1000)  event_event_timestamp,
               to_timestamp(timestamp/1000)  event_created_timestamp,
               '' traffic_id,
               0 session_listing_page_views,
               0 session_product_page_views,
               0 session_photo_page_views

             FROM
             orders where 1=0
                """,
    timestamp_field="event_event_timestamp",
    created_timestamp_column="event_created_timestamp",
)

kafka_traffic = KafkaSource(
    name="traffic",
    kafka_bootstrap_servers="localhost:9092",
    topic="traffic",
    timestamp_field="timestamp",
    batch_source=batch_traffic,
    message_format=JsonFormat(
        schema_json="user_id string, event string, event_event_timestamp timestamp, event_created_timestamp timestamp, "
                    "traffic_id string, session_listing_page_views int, session_product_page_views int, "
                    "session_photo_page_views int"
    ),
    watermark_delay_threshold=timedelta(minutes=5),
)

# --- FEATURE VIEWS ---

order_details_view = FeatureView(
    name="order_details",
    entities=[user, order],
    ttl=timedelta(days=1000),

    schema=[
        Field(name="order_id", dtype=Int64),
        Field(name="user_id", dtype=Int64),
        Field(name="country", dtype=String),
        Field(name="platform", dtype=String),
    ],
    online=True,
    source=orders,
    tags={},
)

user_traffic_view = FeatureView(
    name="user_traffic",
    entities=[user, traffic],
    ttl=timedelta(days=1000),

    schema=[
        Field(name="user_id", dtype=String),
        Field(name="traffic_id", dtype=String),
        Field(name="event", dtype=String),
        Field(name="event_event_timestamp", dtype=UnixTimestamp),
        Field(name="event_created_timestamp", dtype=UnixTimestamp),
        Field(name="session_listing_page_views", dtype=Int64),
        Field(name="session_product_page_views", dtype=Int64),
        Field(name="session_photo_page_views", dtype=Int64),

    ],
    online=True,
    source=kafka_traffic,
    tags={},
)
