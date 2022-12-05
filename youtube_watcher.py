#!/usr/bin/env python
import json
import logging
import sys
from pprint import pformat

import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from config import config


def fetch_playlist_items_page(google_api_key, youtube_playlist_id, page_token=None):

    response = requests.get(
        "https://www.googleapis.com/youtube/v3/playlistItems",
        params={
            "key": google_api_key,
            "playlistId": youtube_playlist_id,
            # content details to allow finding each video's ID
            "part": "contentDetails",
            "pageToken": page_token,
        },
    )

    payload = json.loads(response.text)
    # So that it returns a structured python object rather than raw strings.

    logging.debug(f"Got -> {payload}")

    return payload


def fetch_playlist_items(google_api_key, youtube_playlist_id, page_token=None):
    # fetch one page. Starts from the 1st page if you do not give a page token.
    payload = fetch_playlist_items_page(google_api_key, youtube_playlist_id, page_token)

    # serve items from that page
    # Python Generator, similar to Kafka event stream. Want to treat it as a potentially infinite stream of data to unpage the results from the YT API call.
    # Basically keep getting from the list, until you have stopped asking for it.
    yield from payload["items"]

    # if another page:
    next_page_token = payload.get("nextPageToken")
    if next_page_token is not None:
        # carry on from there
        yield from fetch_playlist_items(google_api_key, youtube_playlist_id, next_page_token)


def fetch_videos(google_api_key, youtube_playlist_id, page_token=None):
    # fetch one page. Starts from the 1st page if you do not give a page token.
    payload = fetch_videos_page(google_api_key, youtube_playlist_id, page_token)

    # serve items from that page
    # Python Generator, similar to Kafka event stream. Want to treat it as a potentially infinite stream of data to unpage the results from the YT API call.
    # Basically keep getting from the list, until you have stopped asking for it.
    yield from payload["items"]

    # if another page:
    next_page_token = payload.get("nextPageToken")
    if next_page_token is not None:
        # carry on from there
        yield from fetch_playlist_items(google_api_key, youtube_playlist_id, next_page_token)


def fetch_videos_page(google_api_key, video_id, page_token=None):

    response = requests.get(
        "https://www.googleapis.com/youtube/v3/videos",
        params={
            "key": google_api_key,
            "id": video_id,
            "part": "snippet,statistics",
            "pageToken": page_token,
        },
    )

    payload = json.loads(response.text)
    # So that it returns a structured python object rather than raw strings.

    logging.debug(f"Got -> {payload}")

    return payload


def summarise_video(video):
    return {
        "video_id": video["id"],
        "title": video["snippet"]["title"],
        # default to 0 if statistic not present
        "views": int(video["statistics"].get("viewCount", 0)),
        "likes": int(video["statistics"].get("likeCount", 0)),
        "comments": int(video["statistics"].get("commentCount", 0)),
    }


def on_delivery(err, record):
    pass


def main():
    logging.info("START")

    schema_registry_client = SchemaRegistryClient(config["schema_registry"])
    youtube_videos_values_schema = schema_registry_client.get_latest_version("youtube_videos-value")

    kafka_config = config["kafka"] | {
        "key.serializer": StringSerializer(),
        "value.serializer": AvroSerializer(
            schema_registry_client,
            youtube_videos_values_schema.schema.schema_str,
        ),
    }
    producer = SerializingProducer(kafka_config)

    google_api_key = config["google_api_key"]
    youtube_playlist_id = config["youtube_playlist_id"]

    # Recur through pages
    for video_item in fetch_playlist_items(google_api_key, youtube_playlist_id):
        logging.info("GOT %s", video_item)
        # fetch the actual videos.
        video_id = video_item["contentDetails"]["videoId"]
        for video in fetch_videos(google_api_key, video_id):
            # pformat for pretty printing
            logging.info("Got video %s", pformat(summarise_video(video)))

            producer.produce(
                topic="youtube_videos",
                key=video_id,
                value={
                    "TITLE": video["snippet"]["title"],
                    # default to 0 if statistic not present
                    "VIEWS": int(video["statistics"].get("viewCount", 0)),
                    "LIKES": int(video["statistics"].get("likeCount", 0)),
                    "COMMENTS": int(video["statistics"].get("commentCount", 0)),
                },
                on_delivery=on_delivery,
            )

    producer.flush()


if __name__ == "__main__":
    # being invoked as a script
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
