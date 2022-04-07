#!/usr/bin/env python3
import os
import json

import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

import requests
from requests.adapters import HTTPAdapter, Retry

from typing import List, Dict
from datetime import datetime


REQUIRED_CONFIG_KEYS = ["installation_id", "apiuser_email", "apiuser_token"]
VERSION = "v2"
BASE_URL = f"https://preprod.cashpad.net/api/salesdata/{VERSION}/"
LOGGER = singer.get_logger()

# This will allow to query with requests_retries which include exponetial backoff retry instead of requests
requests_session = requests.Session()
http_retry_codes = (500, 502, 503, 504, 506, 507, 520, 521, 522, 523, 524, 525, 527, 429, 444, 498, 499)
requests_retries = Retry(total=5, backoff_factor=10, status_forcelist=http_retry_codes)
requests_session.mount('', HTTPAdapter(max_retries=requests_retries))


def get_abs_path(path: str) -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas() -> Dict:
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover() -> object:
    """ This discover schemas from shcemas folder and generates streams from them """
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key="sequential_id",
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method="INCREMENTAL",
            )
        )
    return Catalog(streams)


def get_closure_list(config: Dict, start_sequential_id: int = None) -> List:
    """ This send a list of closure we have to retrieve. It uses start_sequential_id and add 1 to it to get only new
    available closure from Cashpad. Indeed Cashpad start from the start_sequential_id given and will send again data we
    already wrote.
    Example: If start_sequential_id = 1 cashpad will send closure 1, 2, 3 ... Where we just want closure 2, 3 ... """

    archive_url = BASE_URL + config.get("installation_id") + "/archives"
    params = {
        "apiuser_email": config.get("apiuser_email"),
        "apiuser_token": config.get("apiuser_token"),
    }
    if start_sequential_id:
        params["start_sequential_id"] = start_sequential_id + 1 # Add 1 to get next id

    closure_list = []
    response = requests_session.get(archive_url, params=params)
    LOGGER.info(f"response status code is : {response.status_code}")
    LOGGER.info(f"response is : {response.text}")

    if response.status_code == 200:
        for row in response.json().get("data"):
            closure_list.append(
                {
                    "sequential_id": row.get("sequential_id"),
                    "range_begin_date": row.get("range_begin_date"),
                    "range_end_date": row.get("range_end_date")
                }
            )
    return closure_list


def get_live_closing(config: Dict, sequential_id : int = None, version : int = None) -> Dict:
    """ Get content for not closed data. Data sent as output as not fully consolidated and can change in a later call
    We will add missing items to match archive_content.json schema and is_closed flag to false to mark these data are
    live """

    archive_url = BASE_URL + config.get("installation_id") + "/live_data"
    params = {
        "apiuser_email": config.get("apiuser_email"),
        "apiuser_token": config.get("apiuser_token"),
    }

    if sequential_id:
        params["sequential_id"] = sequential_id
    if version:
        params["version"] = version

    response = requests_session.get(archive_url, params=params)

    if response.status_code == 200:
        live_data = response.json().get("data")

        # Case where no new live data
        if not live_data:
            LOGGER.info("No new live data available")
            return []

        # These items are not sent by API, we fill them with None data to avoid error while writing data with singer.
        live_data["id"] = None
        live_data["sequential_id"] = None
        live_data["date_created"] = None
        live_data["user"] = None
        live_data["range_begin_date"] = None
        live_data["range_end_date"] = None

        # Mark data are ongoing
        live_data["is_closed"] = False

        return live_data


def get_closed(config: Dict, closure_list: List) -> List:
    """ Get archive content for a closure list """
    archive_content = BASE_URL + config.get("installation_id") + "/archive_content"
    content_list = []
    for closed in closure_list:
        params = {
            "sequential_id": closed.get("sequential_id"),
            "apiuser_email": config.get("apiuser_email"),
            "apiuser_token": config.get("apiuser_token"),
        }
        response = requests_session.get(archive_content, params=params)

        LOGGER.info(f"response status code is : {response.status_code}")
        LOGGER.info(f"response is : {response.text}")

        if response.status_code == 200:
            content_list.append(response.json().get("data"))
        elif response.status_code == 400:
            LOGGER.error(f"The sequential_id: {closed.get('sequential_id')} requested doesn't exist")
            continue
    return content_list


def sync(config: Dict, state: Dict, catalog: object) -> None:
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    batch_write_timestamp = datetime.now().__str__()

    for stream in catalog.get_selected_streams(state):

        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        tap_data = []
        is_sorted = True
        start_sequential_id = None
        # Here we get list of closed data, that means the data retrived by this function are unmutable and can be
        # ingested by target safely
        if state.get("value"):
            start_sequential_id = state.get("value").get(stream.tap_stream_id)
        closure_list = get_closure_list(config, start_sequential_id)

        # Here we get list of ongoing data, that means the data retrived by this function are mutable and can change by
        # the time you call Cashpad API. We mark these data as not closed and append them to the target write.
        # Data analyst should take care of them later thanks to the ingestion date in order to get only fresh data.
        live_data = get_live_closing(config)

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        # Case where no more closed data
        if len(closure_list) == 0:
            LOGGER.info("No new closed data available")
        else:
            # get_closed return a list of list so we just extend it in tap_data
            tap_data.extend(get_closed(config, closure_list))

        # live_data contain just one list we can append to tap_data
        tap_data.append(live_data)

        # Case where no new data at all
        if len(tap_data) == 0:
            LOGGER.info("No new data available")

        max_bookmark = None
        for row in tap_data:
            row["ingestion_date"] = batch_write_timestamp
            print("is closed : ", row.get("is_closed"))
            row["is_closed"] = row.get("is_closed") if row.get("is_closed") is False else True
            # Write row to the stream for target :
            singer.write_records(stream.tap_stream_id, [row])
            if bookmark_column and row.get("is_closed"):
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state({stream.tap_stream_id: row[bookmark_column]})
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, row[bookmark_column])
        if bookmark_column and not is_sorted and row.get("is_closed"):
            singer.write_state({stream.tap_stream_id: max_bookmark})
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
